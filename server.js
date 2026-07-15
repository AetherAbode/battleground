require('dotenv').config();
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const fs = require('fs-extra');
const path = require('path');
const { exec, spawn } = require('child_process');
const archiver = require('archiver');
const { v4: uuidv4 } = require('uuid');
const rateLimit = require('express-rate-limit');

const app = express();
app.use(cors());
app.use(express.json({ limit: '50mb' }));

const PORT = process.env.PORT || 3000;
const USER_TOKEN = process.env.DEEPSEEK_TOKEN;
const WORKSPACE_ROOT = path.join(__dirname, 'workspace');

// Rate limiting (prevent abuse)
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // limit each IP to 100 requests per windowMs
  message: { error: 'Too many requests, please try again later.' }
});
app.use('/api/', limiter); // apply to all API routes

fs.ensureDirSync(WORKSPACE_ROOT);

// ─── DEEPSEEK PROXY (unchanged) ──────────────────────────────────
if (!USER_TOKEN) {
  console.error('❌ DEEPSEEK_TOKEN is not set.');
  process.exit(1);
}
const DEEPSEEK_WEB_API = 'https://chat.deepseek.com/api/v0/chat/completions';

app.post('/api/chat', async (req, res) => {
  const { messages, model = 'deepseek-v4-pro', stream = true, temperature = 0.7, reasoning_effort } = req.body;
  const payload = { model, messages, stream, temperature, max_tokens: 4096 };
  if (reasoning_effort && model === 'deepseek-v4-pro') payload.reasoning_effort = reasoning_effort;

  try {
    const response = await axios({
      method: 'post',
      url: DEEPSEEK_WEB_API,
      headers: {
        'Authorization': `Bearer ${USER_TOKEN}`,
        'Content-Type': 'application/json',
        'Accept': 'text/event-stream',
        'User-Agent': 'Mozilla/5.0'
      },
      data: payload,
      responseType: stream ? 'stream' : 'json',
      timeout: 120000,
    });

    if (!stream) return res.json(response.data);

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');

    let buffer = '';
    response.data.on('data', (chunk) => {
      buffer += chunk.toString();
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';
      for (const line of lines) {
        if (line.trim() === '') continue;
        if (line.startsWith('data: ')) {
          const data = line.slice(6);
          if (data === '[DONE]') { res.write('data: [DONE]\n\n'); continue; }
          res.write(`data: ${data}\n\n`);
        }
      }
    });
    response.data.on('end', () => {
      if (buffer.trim() && buffer.startsWith('data: ')) {
        const data = buffer.slice(6);
        if (data !== '[DONE]') res.write(`data: ${data}\n\n`);
      }
      res.end();
    });
    response.data.on('error', (err) => {
      console.error('Stream error:', err.message);
      res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
      res.end();
    });
    req.on('close', () => response.data.destroy());
  } catch (error) {
    console.error('Proxy error:', error.message);
    if (error.response) {
      const status = error.response.status;
      const errorMsg = error.response.data?.error?.message || error.message;
      return res.status(status).json({ error: errorMsg });
    }
    res.status(500).json({ error: error.message });
  }
});

// ─── FILE SYSTEM API (with validation) ──────────────────────────
function resolveWorkspacePath(relativePath) {
  // Prevent path traversal
  const safePath = path.normalize(relativePath).replace(/^(\.\.(\/|$))+/, '');
  const resolved = path.resolve(WORKSPACE_ROOT, safePath);
  if (!resolved.startsWith(WORKSPACE_ROOT)) throw new Error('Path traversal not allowed');
  return resolved;
}

app.get('/api/tree', async (req, res) => {
  try {
    const relative = req.query.path || '/';
    const fullPath = resolveWorkspacePath(relative);
    const stat = await fs.stat(fullPath);
    if (!stat.isDirectory()) {
      return res.json({ type: 'file', name: path.basename(fullPath) });
    }
    const items = await fs.readdir(fullPath);
    const tree = [];
    for (const item of items) {
      const itemPath = path.join(fullPath, item);
      const itemStat = await fs.stat(itemPath);
      tree.push({
        name: item,
        type: itemStat.isDirectory() ? 'directory' : 'file',
        path: path.join(relative, item).replace(/\\/g, '/')
      });
    }
    res.json(tree);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/read', async (req, res) => {
  try {
    const filePath = resolveWorkspacePath(req.query.path || '');
    const content = await fs.readFile(filePath, 'utf8');
    res.json({ content });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/write', async (req, res) => {
  try {
    const { path: filePath, content } = req.body;
    const fullPath = resolveWorkspacePath(filePath);
    await fs.ensureDir(path.dirname(fullPath));
    await fs.writeFile(fullPath, content, 'utf8');
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/delete', async (req, res) => {
  try {
    const { path: targetPath } = req.body;
    const fullPath = resolveWorkspacePath(targetPath);
    await fs.remove(fullPath);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/mkdir', async (req, res) => {
  try {
    const { path: dirPath } = req.body;
    const fullPath = resolveWorkspacePath(dirPath);
    await fs.ensureDir(fullPath);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── CODE EXECUTION (with timeout) ──────────────────────────────

app.post('/api/run', async (req, res) => {
  const { filePath, language, args = [] } = req.body;
  const fullPath = resolveWorkspacePath(filePath);

  try {
    await fs.access(fullPath, fs.constants.R_OK);
  } catch {
    return res.status(404).json({ error: 'File not found' });
  }

  let command, cmdArgs;
  const timeout = 10000; // 10 seconds

  switch (language) {
    case 'python':
      command = 'python3';
      cmdArgs = [fullPath, ...args];
      break;
    case 'javascript':
      command = 'node';
      cmdArgs = [fullPath, ...args];
      break;
    case 'java':
      // Compile
      const dir = path.dirname(fullPath);
      const className = path.basename(fullPath, '.java');
      await new Promise((resolve, reject) => {
        exec(`javac ${fullPath}`, { cwd: dir, timeout }, (err, stdout, stderr) => {
          if (err) reject(stderr || err.message);
          else resolve();
        });
      });
      command = 'java';
      cmdArgs = ['-cp', dir, className, ...args];
      break;
    case 'cpp':
      const outPath = path.join(path.dirname(fullPath), path.basename(fullPath, '.cpp'));
      await new Promise((resolve, reject) => {
        exec(`g++ ${fullPath} -o ${outPath}`, { timeout }, (err, stdout, stderr) => {
          if (err) reject(stderr || err.message);
          else resolve();
        });
      });
      command = outPath;
      cmdArgs = args;
      break;
    default:
      return res.status(400).json({ error: 'Unsupported language' });
  }

  const child = spawn(command, cmdArgs, { timeout, cwd: path.dirname(fullPath) });
  let stdout = '', stderr = '';
  child.stdout.on('data', (data) => { stdout += data.toString(); });
  child.stderr.on('data', (data) => { stderr += data.toString(); });
  child.on('close', (code) => {
    res.json({
      stdout,
      stderr,
      exitCode: code,
    });
  });
  child.on('error', (err) => {
    res.status(500).json({ error: err.message });
  });
});

// ─── PACKAGE INSTALL (with validation) ──────────────────────────

app.post('/api/install', async (req, res) => {
  const { packageName, language = 'python' } = req.body;
  // Basic validation: avoid arbitrary command injection
  if (!/^[a-zA-Z0-9_.-]+$/.test(packageName)) {
    return res.status(400).json({ error: 'Invalid package name.' });
  }
  let command, args;
  if (language === 'python') {
    command = 'pip3';
    args = ['install', packageName];
  } else if (language === 'javascript') {
    command = 'npm';
    args = ['install', packageName, '--save-dev'];
  } else {
    return res.status(400).json({ error: 'Unsupported language for package install' });
  }

  exec(`${command} ${args.join(' ')}`, { cwd: WORKSPACE_ROOT, timeout: 60000 }, (err, stdout, stderr) => {
    if (err) {
      return res.status(500).json({ error: stderr || err.message });
    }
    res.json({ success: true, stdout, stderr });
  });
});

// ─── PROJECT IMPORT/EXPORT ──────────────────────────────────────

app.get('/api/project/export', async (req, res) => {
  const archive = archiver('zip', { zlib: { level: 9 } });
  archive.directory(WORKSPACE_ROOT, false);
  archive.on('error', (err) => { throw err; });
  res.attachment('workspace.zip');
  archive.pipe(res);
  archive.finalize();
});

app.post('/api/project/import', async (req, res) => {
  const { zip } = req.body;
  const buffer = Buffer.from(zip, 'base64');
  const tempZip = path.join(__dirname, 'temp.zip');
  await fs.writeFile(tempZip, buffer);
  const extract = require('extract-zip');
  await extract(tempZip, { dir: WORKSPACE_ROOT });
  await fs.remove(tempZip);
  res.json({ success: true });
});

// ─── HEALTH CHECK ──────────────────────────────────────────────

app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    message: 'Full coding sandbox backend',
    version: '2.0.0'
  });
});

app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`   Workspace: ${WORKSPACE_ROOT}`);
});
