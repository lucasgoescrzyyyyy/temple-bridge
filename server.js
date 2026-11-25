const http = require('http');
const { WebSocketServer } = require('ws');
const connector = require('tiktok-live-connector');
const TikTokConnection = connector.TikTokLiveConnection || connector.WebcastPushConnection;

const clients = new Set();

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === '/test') {
    const type = url.searchParams.get('type') || 'gift';
    const payload = type === 'chat'
      ? {
          event: 'chat',
          data: { uniqueId: 'test_donor', nickname: 'မောင်အောင်', comment: 'သာဓု သာဓု သာဓု 🙏' }
        }
      : {
          event: 'gift',
          data: { uniqueId: 'test_donor', nickname: 'မောင်အောင်', giftName: 'Rose', diamondCount: 15, repeatCount: 1, repeatEnd: true, giftType: 0 }
        };

    const msg = JSON.stringify(payload);
    for (const c of clients) {
      if (c.ws.readyState === 1) c.ws.send(msg);
    }
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    return res.end(`Sent test ${type} to ${clients.size} screen(s)`);
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Temple Live Bridge OK');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const username = (url.searchParams.get('user') || '').replace(/^@/, '').trim();
  const client = { ws, username };
  clients.add(client);

  let tiktok = null;
  let retryTimer = null;
  let closed = false;

  const send = (obj) => {
    if (ws.readyState === 1) ws.send(JSON.stringify(obj));
  };

  async function startTikTok() {
    if (closed || !username) {
      send({ event: 'status', connected: false, text: 'No TikTok username' });
      return;
    }

    try {
      if (tiktok) { try { tiktok.disconnect(); } catch {} }
      tiktok = new TikTokConnection(username);

      tiktok.on('gift', (d) => {
        const uniqueId = d.uniqueId || d.user?.uniqueId || d.userId || 'guest';
        const nickname = d.nickname || d.user?.nickname || uniqueId;
        send({
          event: 'gift',
          data: {
            uniqueId,
            nickname,
            giftName: d.giftName || d.giftDetails?.giftName || 'Gift',
            diamondCount: d.diamondCount || d.giftDetails?.diamondCount || 1,
            repeatCount: d.repeatCount || 1,
            repeatEnd: d.repeatEnd ?? true,
            giftType: d.giftType ?? d.giftDetails?.giftType ?? 0
          }
        });
      });

      tiktok.on('chat', (d) => {
        const uniqueId = d.uniqueId || d.user?.uniqueId || d.userId || 'guest';
        const nickname = d.nickname || d.user?.nickname || uniqueId;
        send({
          event: 'chat',
          data: { uniqueId, nickname, comment: d.comment || '' }
        });
      });

      tiktok.on('disconnected', () => {
        if (closed) return;
        send({ event: 'status', connected: false, text: 'TikTok: Reconnecting...' });
        retryTimer = setTimeout(startTikTok, 15000);
      });

      await tiktok.connect();
      send({ event: 'status', connected: true, text: 'Connected' });
    } catch {
      if (closed) return;
      send({ event: 'status', connected: false, text: `Waiting for @${username} LIVE...` });
      retryTimer = setTimeout(startTikTok, 15000);
    }
  }

  startTikTok();

  ws.on('close', () => {
    closed = true;
    clients.delete(client);
    if (retryTimer) clearTimeout(retryTimer);
    if (tiktok) { try { tiktok.disconnect(); } catch {} }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Bridge running on ${PORT}`));
