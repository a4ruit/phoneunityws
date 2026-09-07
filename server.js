const http = require('http');
const fs = require('fs');
const { WebSocketServer } = require('ws');

// Serve the web page.
const server = http.createServer((req, res) => {
  const file = req.url === '/' ? '/index.html' : req.url;
  fs.readFile(__dirname + file, (err, data) => {
    if (err) { res.writeHead(404).end('not found'); return; }
    if (file.endsWith('.css')) res.setHeader('Content-Type', 'text/css');
    res.end(data);
  });
});

// Relay every message to all the other connected clients.
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
  ws.on('message', (msg) => {
    for (const client of wss.clients) {
      if (client !== ws && client.readyState === client.OPEN) {
        client.send(msg.toString());
      }
    }
  });
});

server.listen(process.env.PORT || 3000);
