'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, 'public');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon'
};

// ---------------------------------------------------------------- static site

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    return;
  }

  const rel = url.pathname === '/' ? 'index.html' : url.pathname.replace(/^\/+/, '');
  const file = path.join(PUBLIC_DIR, path.normalize(rel));

  if (!file.startsWith(PUBLIC_DIR)) {
    res.writeHead(403).end('forbidden');
    return;
  }

  fs.readFile(file, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain' }).end('not found');
      return;
    }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream' });
    res.end(data);
  });
});

// ------------------------------------------------------------------ websocket

const wss = new WebSocketServer({ server, path: '/ws' });

/** room name -> { clients: Set<ws>, lastHeading: object|null } */
const rooms = new Map();

function getRoom(name) {
  let room = rooms.get(name);
  if (!room) {
    room = { clients: new Set(), lastHeading: null };
    rooms.set(name, room);
  }
  return room;
}

function broadcast(room, sender, payload) {
  const text = JSON.stringify(payload);
  for (const client of room.clients) {
    if (client !== sender && client.readyState === client.OPEN) {
      client.send(text);
    }
  }
}

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const roomName = (url.searchParams.get('room') || 'default').slice(0, 64);
  const role = url.searchParams.get('role') === 'phone' ? 'phone' : 'viewer';

  const room = getRoom(roomName);
  room.clients.add(ws);
  ws.isAlive = true;

  ws.send(JSON.stringify({ type: 'welcome', room: roomName, role, peers: room.clients.size - 1 }));

  // A viewer joining mid-session gets the last known heading immediately.
  if (role === 'viewer' && room.lastHeading) {
    ws.send(JSON.stringify(room.lastHeading));
  }

  broadcast(room, ws, { type: 'peers', count: room.clients.size });

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (!msg || typeof msg !== 'object') return;

    if (msg.type === 'heading') {
      const payload = {
        type: 'heading',
        heading: Number(msg.heading) || 0,
        pitch: Number(msg.pitch) || 0,
        roll: Number(msg.roll) || 0,
        absolute: Boolean(msg.absolute),
        ts: Date.now()
      };
      room.lastHeading = payload;
      broadcast(room, ws, payload);
      return;
    }

    // Entity positions from Unity, relayed to the phones that render the audio.
    if (msg.type === 'entities') {
      broadcast(room, ws, msg);
      return;
    }

    // One-shot cue (an entity entered the facing cone). Distinct from the
    // ping/pong keepalive below.
    if (msg.type === 'cue') {
      broadcast(room, ws, {
        type: 'cue',
        x: Number(msg.x) || 0,
        z: Number(msg.z) || 0
      });
      return;
    }

    if (msg.type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong', ts: Date.now() }));
    }
  });

  ws.on('close', () => {
    room.clients.delete(ws);
    if (room.clients.size === 0) {
      rooms.delete(roomName);
    } else {
      broadcast(room, ws, { type: 'peers', count: room.clients.size });
    }
  });
});

// Render closes idle connections; keep them warm and drop dead ones.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, () => {
  console.log(`heading relay listening on :${PORT}`);
});
