require('dotenv').config();
const express = require('express');
const cors = require('cors');
const axios = require('axios');

const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' }));

const PORT = process.env.PORT || 3000;
const USER_TOKEN = process.env.DEEPSEEK_TOKEN;

// --- Validate token on startup ---
if (!USER_TOKEN) {
  console.error('❌ DEEPSEEK_TOKEN environment variable is not set.');
  console.error('Please add it in Railway: Variables → DEEPSEEK_TOKEN');
  process.exit(1);
}

// DeepSeek web chat API endpoint (reverse-engineered)
const DEEPSEEK_WEB_API = 'https://chat.deepseek.com/api/v0/chat/completions';

// --- Health check endpoint (for Railway) ---
app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    message: 'DeepSeek proxy is running',
    version: '1.0.0',
    model: 'deepseek-v4-pro'
  });
});

// --- Main chat endpoint ---
app.post('/api/chat', async (req, res) => {
  const { 
    messages, 
    model = 'deepseek-v4-pro', 
    stream = true, 
    temperature = 0.7, 
    reasoning_effort,
    max_tokens = 4096 
  } = req.body;

  // Validate input
  if (!messages || !Array.isArray(messages) || messages.length === 0) {
    return res.status(400).json({ error: 'Invalid messages array' });
  }

  const payload = {
    model,
    messages,
    stream,
    temperature,
    max_tokens,
  };

  // Add reasoning_effort only for pro model
  if (reasoning_effort && model === 'deepseek-v4-pro') {
    payload.reasoning_effort = reasoning_effort;
  }

  try {
    const response = await axios({
      method: 'post',
      url: DEEPSEEK_WEB_API,
      headers: {
        'Authorization': `Bearer ${USER_TOKEN}`,
        'Content-Type': 'application/json',
        'Accept': 'text/event-stream',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      },
      data: payload,
      responseType: stream ? 'stream' : 'json',
      timeout: 120000, // 2 minutes timeout
    });

    if (!stream) {
      return res.json(response.data);
    }

    // --- Streaming: forward SSE events ---
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no'); // Disable nginx buffering

    let buffer = '';

    response.data.on('data', (chunk) => {
      buffer += chunk.toString();
      const lines = buffer.split('\n');
      buffer = lines.pop() || ''; // Keep incomplete line

      for (const line of lines) {
        if (line.trim() === '') continue;
        if (line.startsWith('data: ')) {
          const data = line.slice(6);
          if (data === '[DONE]') {
            res.write('data: [DONE]\n\n');
            continue;
          }
          res.write(`data: ${data}\n\n`);
        } else if (line.startsWith('event: ')) {
          // Forward event type if needed
          res.write(`${line}\n`);
        }
      }
    });

    response.data.on('end', () => {
      // Send any remaining buffer
      if (buffer.trim() && buffer.startsWith('data: ')) {
        const data = buffer.slice(6);
        if (data !== '[DONE]') {
          res.write(`data: ${data}\n\n`);
        }
      }
      res.end();
    });

    response.data.on('error', (err) => {
      console.error('Stream error:', err.message);
      res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
      res.end();
    });

    // Handle client disconnect
    req.on('close', () => {
      response.data.destroy();
    });

  } catch (error) {
    console.error('Proxy error:', error.message);
    
    if (error.response) {
      // Forward status and error details from DeepSeek
      const status = error.response.status;
      const errorMsg = error.response.data?.error?.message || error.message;
      
      if (status === 401) {
        console.error('❌ Authentication failed. Check your DEEPSEEK_TOKEN.');
        return res.status(401).json({ 
          error: 'Authentication failed. Please check your token.',
          details: errorMsg
        });
      }
      
      if (status === 429) {
        return res.status(429).json({ 
          error: 'Rate limited. Please try again later.',
          details: errorMsg
        });
      }
      
      return res.status(status).json({ 
        error: errorMsg,
        status: status
      });
    }
    
    res.status(500).json({ error: error.message });
  }
});

// --- Error handling middleware ---
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err.stack);
  res.status(500).json({ error: 'Internal server error' });
});

// --- Start server ---
app.listen(PORT, () => {
  console.log(`🚀 DeepSeek proxy running on port ${PORT}`);
  console.log(`   Token: ${USER_TOKEN.slice(0, 10)}...${USER_TOKEN.slice(-6)}`);
  console.log(`   Health check: http://localhost:${PORT}/`);
  console.log(`   Chat endpoint: http://localhost:${PORT}/api/chat`);
});
