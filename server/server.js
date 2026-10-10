'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const DIRECTIONS = new Set(['down', 'left', 'right', 'up']);
const MAX_PLAYERS = 30;
const MAX_PAYLOAD_BYTES = 1024;
const SNAPSHOT_INTERVAL_MS = 66;
const HEARTBEAT_INTERVAL_MS = 30000;
// Loose limit for now; tightened once the final room size is known.
const COORD_LIMIT = 5000;

function normalizeSpriteConfig(config) {
  const users = Object.create(null);

  for (const [key, file] of Object.entries((config && config.users) || {})) {
    users[String(key).replace(/^@/, '').trim().toLowerCase()] = String(file);
  }

  return {
    defaultSprite: String((config && config.default) || 'sprites.png'),
    users
  };
}

function loadSpriteConfig() {
  const raw = fs.readFileSync(path.join(__dirname, 'sprites.json'), 'utf8');
  return normalizeSpriteConfig(JSON.parse(raw));
}

function cleanName(value) {
  if (typeof value !== 'string') {
    return '';
  }

  return value.replace(/[\u0000-\u001f]/g, '').trim().slice(0, 32);
}

function cleanUsername(value) {
  if (typeof value !== 'string') {
    return '';
  }

  return value.replace(/^@/, '').trim().toLowerCase().slice(0, 64);
}

function cleanCoord(value) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return null;
  }

  return Math.max(-COORD_LIMIT, Math.min(COORD_LIMIT, value));
}

function spriteFor(sprites, username, tgId) {
  if (username && sprites.users[username]) {
    return sprites.users[username];
  }

  if (tgId !== undefined && tgId !== null && sprites.users[String(tgId)]) {
    return sprites.users[String(tgId)];
  }

  return sprites.defaultSprite;
}

function applyState(player, msg) {
  let changed = false;

  const x = cleanCoord(msg.x);
  const y = cleanCoord(msg.y);

  if (x !== null && x !== player.x) {
    player.x = x;
    changed = true;
  }

  if (y !== null && y !== player.y) {
    player.y = y;
    changed = true;
  }

  if (DIRECTIONS.has(msg.dir) && msg.dir !== player.dir) {
    player.dir = msg.dir;
    changed = true;
  }

  if (typeof msg.moving === 'boolean' && msg.moving !== player.moving) {
    player.moving = msg.moving;
    changed = true;
  }

  return changed;
}

function publicPlayer(player) {
  return {
    id: player.id,
    name: player.name,
    sprite: player.sprite,
    x: player.x,
    y: player.y,
    dir: player.dir,
    moving: player.moving
  };
}

function start(port, options = {}) {
  const sprites = options.spriteConfig
    ? normalizeSpriteConfig(options.spriteConfig)
    : loadSpriteConfig();

  const musicEpoch = Date.now();
  const players = new Map();
  const sockets = new Map();
  let nextId = 1;
  let dirty = false;

  const httpServer = http.createServer((req, res) => {
    res.writeHead(200, {
      'Content-Type': 'text/plain',
      'Access-Control-Allow-Origin': '*'
    });
    res.end('PixelTest server ok\n');
  });

  const wss = new WebSocketServer({
    server: httpServer,
    maxPayload: MAX_PAYLOAD_BYTES
  });

  function send(ws, message) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
    }
  }

  function broadcast(message, except) {
    const data = JSON.stringify(message);

    for (const ws of sockets.keys()) {
      if (ws !== except && ws.readyState === WebSocket.OPEN) {
        ws.send(data);
      }
    }
  }

  function removePlayer(ws) {
    const id = sockets.get(ws);

    if (!id) {
      return;
    }

    sockets.delete(ws);
    players.delete(id);
    broadcast({ type: 'left', id });
    dirty = true;
  }

  function handleMessage(ws, msg) {
    if (msg.type === 'ping') {
      send(ws, {
        type: 'pong',
        t: typeof msg.t === 'number' ? msg.t : null,
        serverTime: Date.now()
      });
      return;
    }

    if (msg.type === 'join') {
      if (sockets.has(ws)) {
        return;
      }

      if (players.size >= MAX_PLAYERS) {
        send(ws, { type: 'full' });
        ws.close(1013, 'server full');
        return;
      }

      const username = cleanUsername(msg.username);
      const dir = DIRECTIONS.has(msg.dir) ? msg.dir : 'down';

      const player = {
        id: 'p' + nextId++,
        name: cleanName(msg.name) || 'Player',
        sprite: spriteFor(sprites, username, msg.tgId),
        x: cleanCoord(msg.x) ?? 0,
        y: cleanCoord(msg.y) ?? 0,
        dir,
        moving: false
      };

      players.set(player.id, player);
      sockets.set(ws, player.id);

      send(ws, {
        type: 'welcome',
        selfId: player.id,
        serverTime: Date.now(),
        musicEpoch,
        players: [...players.values()].map(publicPlayer)
      });

      broadcast({ type: 'joined', player: publicPlayer(player) }, ws);
      return;
    }

    if (msg.type === 'state') {
      const id = sockets.get(ws);
      const player = id && players.get(id);

      if (player && applyState(player, msg)) {
        dirty = true;
      }
    }
  }

  wss.on('connection', ws => {
    ws.isAlive = true;

    ws.on('pong', () => {
      ws.isAlive = true;
    });

    ws.on('message', (data, isBinary) => {
      if (isBinary) {
        return;
      }

      let msg;

      try {
        msg = JSON.parse(data.toString());
      } catch (error) {
        return;
      }

      if (msg && typeof msg.type === 'string') {
        handleMessage(ws, msg);
      }
    });

    ws.on('close', () => removePlayer(ws));
    ws.on('error', () => {});
  });

  const snapshotTimer = setInterval(() => {
    if (!dirty) {
      return;
    }

    dirty = false;

    broadcast({
      type: 'snapshot',
      players: [...players.values()].map(p => ({
        id: p.id,
        x: p.x,
        y: p.y,
        dir: p.dir,
        moving: p.moving
      }))
    });
  }, SNAPSHOT_INTERVAL_MS);

  const heartbeatTimer = setInterval(() => {
    for (const ws of wss.clients) {
      if (!ws.isAlive) {
        ws.terminate();
        continue;
      }

      ws.isAlive = false;
      ws.ping();
    }
  }, HEARTBEAT_INTERVAL_MS);

  return new Promise((resolve, reject) => {
    httpServer.once('error', reject);

    httpServer.listen(port, () => {
      resolve({
        port: httpServer.address().port,
        close: () =>
          new Promise(done => {
            clearInterval(snapshotTimer);
            clearInterval(heartbeatTimer);

            for (const ws of wss.clients) {
              ws.terminate();
            }

            wss.close(() => {
              httpServer.close(() => done());
              if (httpServer.closeAllConnections) {
                httpServer.closeAllConnections();
              }
            });
          })
      });
    });
  });
}

if (require.main === module) {
  const port = Number(process.env.PORT) || 3000;

  start(port).then(server => {
    console.log('PixelTest server listening on port ' + server.port);
  });
}

module.exports = { start };
