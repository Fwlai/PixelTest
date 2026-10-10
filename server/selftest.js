'use strict';

const assert = require('assert');
const http = require('http');
const { WebSocket } = require('ws');
const { start } = require('./server');

function connect(port) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket('ws://127.0.0.1:' + port);
    ws.inbox = [];

    ws.on('message', data => {
      ws.inbox.push(JSON.parse(data.toString()));
      ws.emit('inbox');
    });

    ws.on('open', () => resolve(ws));
    ws.on('error', reject);
  });
}

function waitFor(ws, predicate, timeout = 2000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      ws.off('inbox', check);
      reject(new Error('timeout waiting for message'));
    }, timeout);

    function check() {
      const index = ws.inbox.findIndex(predicate);

      if (index < 0) {
        return false;
      }

      const [msg] = ws.inbox.splice(index, 1);
      clearTimeout(timer);
      ws.off('inbox', check);
      resolve(msg);
      return true;
    }

    if (!check()) {
      ws.on('inbox', check);
    }
  });
}

function sendJson(ws, obj) {
  ws.send(JSON.stringify(obj));
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function httpStatus(port) {
  return new Promise((resolve, reject) => {
    http
      .get({ host: '127.0.0.1', port, path: '/' }, res => {
        res.resume();
        resolve(res.statusCode);
      })
      .on('error', reject);
  });
}

function pass(name) {
  console.log('PASS ' + name);
}

async function main() {
  const server = await start(0, {
    spriteConfig: {
      default: 'sprites.png',
      users: { '@Test_User': 'sprite-test.png', '777': 'sprite-by-id.png' }
    }
  });

  const port = server.port;
  const clients = [];

  try {
    assert.strictEqual(await httpStatus(port), 200);
    pass('health endpoint answers 200');

    const a = await connect(port);
    clients.push(a);
    sendJson(a, { type: 'join', tgId: 1, username: 'Test_User', name: 'Alice', x: 10, y: 20, dir: 'down' });
    const welcomeA = await waitFor(a, m => m.type === 'welcome');
    assert.strictEqual(welcomeA.players.length, 1);
    assert.strictEqual(welcomeA.players[0].sprite, 'sprite-test.png');
    assert.ok(Number.isFinite(welcomeA.serverTime));
    assert.ok(Number.isFinite(welcomeA.musicEpoch));
    pass('welcome carries mapped sprite and clock fields (username, case-insensitive)');

    const b = await connect(port);
    clients.push(b);
    sendJson(b, { type: 'join', username: 'nobody', name: 'Bob', x: 30, y: 40, dir: 'left' });
    const welcomeB = await waitFor(b, m => m.type === 'welcome');
    assert.strictEqual(welcomeB.players.length, 2);
    const selfB = welcomeB.players.find(p => p.id === welcomeB.selfId);
    assert.strictEqual(selfB.sprite, 'sprites.png');
    const joinedB = await waitFor(a, m => m.type === 'joined' && m.player.id === welcomeB.selfId);
    assert.strictEqual(joinedB.player.name, 'Bob');
    pass('unknown user gets default sprite; others are told about the join');

    const c = await connect(port);
    clients.push(c);
    sendJson(c, { type: 'join', tgId: 777, name: 'Cy', x: 0, y: 0, dir: 'up' });
    const welcomeC = await waitFor(c, m => m.type === 'welcome');
    assert.strictEqual(welcomeC.players.find(p => p.id === welcomeC.selfId).sprite, 'sprite-by-id.png');
    c.close();
    await waitFor(a, m => m.type === 'left' && m.id === welcomeC.selfId);
    pass('sprite mapping by numeric ID works; leaving is broadcast');

    sendJson(b, { type: 'state', x: 100, y: 150, dir: 'right', moving: true });
    const snap = await waitFor(a, m => m.type === 'snapshot' && m.players.some(p => p.id === welcomeB.selfId && p.x === 100));
    const moved = snap.players.find(p => p.id === welcomeB.selfId);
    assert.deepStrictEqual([moved.x, moved.y, moved.dir, moved.moving], [100, 150, 'right', true]);
    pass('movement is broadcast to other players');

    a.inbox.length = 0;
    sendJson(b, { type: 'state', x: 'abc', y: null, dir: 'sideways', moving: 'yes' });
    await sleep(250);
    assert.strictEqual(a.inbox.filter(m => m.type === 'snapshot').length, 0);
    pass('invalid state values are ignored');

    sendJson(a, { type: 'ping', t: 123 });
    const pong = await waitFor(a, m => m.type === 'pong' && m.t === 123);
    assert.ok(Number.isFinite(pong.serverTime));
    pass('ping/pong returns server time');

    const closed = new Promise(resolve => b.once('close', code => resolve(code)));
    b.send('x'.repeat(5000));
    const code = await Promise.race([closed, sleep(3000).then(() => 'timeout')]);
    assert.strictEqual(code, 1009);
    await waitFor(a, m => m.type === 'left' && m.id === welcomeB.selfId);
    pass('oversized message closes the connection; others are told');

    console.log('ALL SELFTESTS PASSED');
  } catch (error) {
    console.error('SELFTEST FAILED: ' + error.message);
    process.exitCode = 1;
  } finally {
    for (const ws of clients) {
      ws.terminate();
    }

    await server.close();
  }
}

main();
