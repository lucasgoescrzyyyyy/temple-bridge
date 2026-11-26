const http = require('http');
const { WebSocketServer } = require('ws');
const { WebcastPushConnection } = require('tiktok-live-connector');

const PORT = process.env.PORT || 3000;
const rooms = new Map();

function broadcast(username, payload) {
  const room = rooms.get(username);
  if (!room) return;
  const raw = JSON.stringify(payload);
  for (const ws of room.clients) {
    if (ws.readyState === 1) ws.send(raw);
  }
}

function ensureTikTokConnection(username) {
  let room = rooms.get(username);
  if (!room) {
    room = { clients: new Set(), conn: null, connected: false, retryTimer: null };
    rooms.set(username, room);
  }
  if (room.conn) return room;

  const conn = new WebcastPushConnection(username, {
    processInitialData: true,
    enableExtendedGiftInfo: true,
    requestPollingIntervalMs: 2000
  });
  room.conn = conn;

  const forward = (eventName) => (data) => {
    broadcast(username, { event: eventName, data });
  };

  conn.on('member', forward('member'));
  conn.on('roomUser', forward('roomUser'));
  conn.on('chat', forward('chat'));
  conn.on('gift', forward('gift'));
  conn.on('like', forward('like'));
  conn.on('follow', forward('follow'));
  conn.on('share', forward('share'));
  conn.on('social', forward('social'));

  conn.connect().then(() => {
    room.connected = true;
    broadcast(username, { event: 'status', connected: true, text: `Connected to @${username}` });
  }).catch((err) => {
    room.connected = false;
    room.conn = null;
    broadcast(username, { event: 'status', connected: false, text: `Waiting for @${username} LIVE...` });
    if (room.clients.size > 0) {
      room.retryTimer = setTimeout(() => ensureTikTokConnection(username), 10000);
    }
  });

  conn.on('disconnected', () => {
    room.connected = false;
    room.conn = null;
    broadcast(username, { event: 'status', connected: false, text: `Waiting for @${username} LIVE...` });
    if (room.clients.size > 0) {
      room.retryTimer = setTimeout(() => ensureTikTokConnection(username), 8000);
    }
  });

  return room;
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  if (url.pathname === '/test') {
    const type = url.searchParams.get('type') || 'member';
    const id = 'test_' + Date.now();
    let payload = { event: 'member', data: { uniqueId: id, nickname: 'မောင်အောင်' } };
    if (type === 'gift') {
      payload = { event: 'gift', data: { uniqueId: id, nickname: 'မောင်အောင်', giftName: 'Rose', repeatCount: 1, diamondCount: 1, giftType: 0, repeatEnd: true } };
    } else if (type === 'chat') {
      payload = { event: 'chat', data: { uniqueId: id, nickname: 'မောင်အောင်', comment: 'သာဓု သာဓု သာဓု 🙏' } };
    }
    let count = 0;
    for (const [uname, room] of rooms.entries()) {
      if (room.clients.size > 0) {
        broadcast(uname, payload);
        count += room.clients.size;
      }
    }
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end(`Sent test ${type} to ${count} screen(s)`);
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Temple Bridge OK');
});

const wss = new WebSocketServer({ server });
wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const username = (url.searchParams.get('user') || '').replace(/^@/, '').trim().toLowerCase();
  if (!username) return ws.close();

  const room = ensureTikTokConnection(username);
  room.clients.add(ws);
  ws.send(JSON.stringify({
    event: 'status',
    connected: room.connected,
    text: room.connected ? `Connected to @${username}` : `Waiting for @${username} LIVE...`
  }));

  ws.on('close', () => {
    room.clients.delete(ws);
    if (room.clients.size === 0 && room.conn) {
      try { room.conn.disconnect(); } catch {}
      room.conn = null;
    }
  });
});

server.listen(PORT, () => console.log('Bridge listening on', PORT));
