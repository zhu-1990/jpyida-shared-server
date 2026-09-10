'use strict';
/*
 * 通用实时共享后端（多房间）
 * ------------------------------------------------------------
 * 架构：
 *   - 每个 "房间"(room) = 一个共享项目（如日本货量看板、未来的其他项目）
 *   - 房间文档 = { __version, rows, maps, updated_at }，落盘到 DATA_DIR/<room>.json
 *   - HTTP GET  /api/<room>            取当前文档
 *   - HTTP POST /api/<room>?version=V&force=0  保存文档（乐观锁 + 防误清空）
 *   - WS   /ws?room=<room>             长连接；服务端在文档变更时广播 {type:'changed',version}
 * 前端收到 changed 后重新 GET 最新文档即可，真正实时（秒级）。
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = parseInt(process.env.PORT || '3000', 10);
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const ACCESS_TOKEN = process.env.ACCESS_TOKEN || '';   // 可选：设置后读写都需带 token（?token= 或 x-access-token 头）
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

/* 校验访问令牌：未配置 ACCESS_TOKEN 时直接放行（保持开箱即用） */
function authed(req, url) {
  if (!ACCESS_TOKEN) return true;
  const t = req.headers['x-access-token'] || url.searchParams.get('token') || '';
  return t === ACCESS_TOKEN;
}

function safeRoom(room) {
  return String(room || 'default').replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 64) || 'default';
}
function roomFile(room) { return path.join(DATA_DIR, safeRoom(room) + '.json'); }

function emptyDoc() { return { __version: 0, rows: [], maps: {}, updated_at: null }; }
function readRoom(room) {
  const f = roomFile(room);
  if (!fs.existsSync(f)) return emptyDoc();
  try {
    const d = JSON.parse(fs.readFileSync(f, 'utf8'));
    if (typeof d.__version !== 'number') d.__version = 0;
    if (!Array.isArray(d.rows)) d.rows = [];
    if (!d.maps || typeof d.maps !== 'object') d.maps = {};
    return d;
  } catch (e) { return emptyDoc(); }
}
function writeRoom(room, doc) { fs.writeFileSync(roomFile(room), JSON.stringify(doc)); }

const wss = new WebSocketServer({ noServer: true });
function broadcast(room, msg) {
  const txt = JSON.stringify(msg);
  wss.clients.forEach((c) => {
    if (c._room === room && c.readyState === 1) { try { c.send(txt); } catch (e) {} }
  });
}

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  const url = new URL(req.url, 'http://localhost');
  const m = url.pathname.match(/^\/api\/([^/]+)$/);
  if (m) {
    if (!authed(req, url)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'unauthorized' }));
      return;
    }
    const room = decodeURIComponent(m[1]);
    if (req.method === 'GET') {
      const doc = readRoom(room);
      res.setHeader('Content-Type', 'application/json');
      res.writeHead(200);
      res.end(JSON.stringify(doc));
      return;
    }
    if (req.method === 'POST') {
      let body = '';
      req.on('data', (c) => { body += c; });
      req.on('end', () => {
        let incoming;
        try { incoming = JSON.parse(body); } catch (e) {
          res.writeHead(400); res.end(JSON.stringify({ error: 'invalid_json' })); return;
        }
        const cur = readRoom(room);
        const clientVer = parseInt(url.searchParams.get('version') || '0', 10);
        const force = url.searchParams.get('force') === '1';
        // 乐观锁：版本不一致说明别人已改，返回冲突让前端合并重发
        if (clientVer !== cur.__version) {
          res.setHeader('Content-Type', 'application/json');
          res.writeHead(409);
          res.end(JSON.stringify({ error: 'conflict', current: cur }));
          return;
        }
        // 防误清空：本地为空但云端有数据，且未明确 force 确认 -> 拒绝
        const rowsLen = Array.isArray(incoming.rows) ? incoming.rows.length : 0;
        if (rowsLen === 0 && cur.rows.length > 0 && !force) {
          res.setHeader('Content-Type', 'application/json');
          res.writeHead(403);
          res.end(JSON.stringify({ error: 'empty_rejected', message: '本地为空但云端有数据，需确认清空（带 force=1）' }));
          return;
        }
        const newDoc = {
          __version: cur.__version + 1,
          rows: incoming.rows || [],
          maps: incoming.maps || cur.maps || {},
          updated_at: new Date().toISOString(),
        };
        try { writeRoom(room, newDoc); } catch (e) {
          res.writeHead(500); res.end(JSON.stringify({ error: 'write_failed' })); return;
        }
        broadcast(room, { type: 'changed', version: newDoc.__version, size: rowsLen, at: newDoc.updated_at });
        res.setHeader('Content-Type', 'application/json');
        res.writeHead(200);
        res.end(JSON.stringify({ ok: true, version: newDoc.__version }));
      });
      return;
    }
  }
  if (url.pathname === '/health') { res.writeHead(200); res.end('ok'); return; }
  res.writeHead(404); res.end('not found');
});

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname === '/ws' && authed(req, url)) {
    wss.handleUpgrade(req, socket, head, (ws) => {
      ws._room = url.searchParams.get('room') || 'default';
      wss.emit('connection', ws, req);
    });
  } else { socket.destroy(); }
});

wss.on('connection', (ws) => {
  const cur = readRoom(ws._room);
  try { ws.send(JSON.stringify({ type: 'hello', version: cur.__version, size: cur.rows.length, at: cur.updated_at })); } catch (e) {}
  ws.on('message', (msg) => {
    try {
      const o = JSON.parse(msg);
      if (o.type === 'ping') ws.send(JSON.stringify({ type: 'pong' }));
    } catch (e) {}
  });
  ws.on('close', () => {});
});

server.listen(PORT, () => console.log('[shared-server] listening on ' + PORT + ' data=' + DATA_DIR + ' auth=' + (ACCESS_TOKEN ? 'on' : 'off')));
