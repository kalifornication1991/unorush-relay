const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 8787);
const wss = new WebSocketServer({ port: PORT });

const rooms = new Map();

function makeCode() {
  for (let tries = 0; tries < 1000; tries++) {
    const code = String(Math.floor(100000 + Math.random() * 900000));
    if (!rooms.has(code)) return code;
  }
  throw new Error('Could not create room code');
}

function safeSend(ws, payload) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function playerList(room) {
  return [...room.players.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([seat, p]) => ({
      seat,
      name: p.name,
      avatar: p.avatar,
      ready: p.ready,
    }));
}

function broadcast(room, payload, except = null) {
  for (const p of room.players.values()) {
    if (p.ws !== except) safeSend(p.ws, payload);
  }
}

function broadcastLobby(room) {
  broadcast(room, {
    type: 'lobby',
    code: room.code,
    players: playerList(room),
  });
}

function firstOpenSeat(room) {
  for (let seat = 0; seat < 4; seat++) {
    if (!room.players.has(seat)) return seat;
  }
  return -1;
}

function roomFor(ws) {
  if (!ws.roomCode) return null;
  return rooms.get(ws.roomCode) || null;
}

function leaveRoom(ws) {
  const room = roomFor(ws);
  if (!room) return;

  const seat = ws.seat;
  const leavingWasHost = room.host === ws;

  if (seat >= 0) room.players.delete(seat);

  ws.roomCode = null;
  ws.seat = -1;

  if (leavingWasHost) {
    broadcast(room, {
      type: 'error',
      message: 'The host left the lobby.',
    }, ws);

    for (const p of room.players.values()) {
      try { p.ws.close(1000, 'Host left'); } catch (_) {}
    }

    rooms.delete(room.code);
    return;
  }

  if (room.players.size === 0) {
    rooms.delete(room.code);
  } else {
    broadcastLobby(room);
  }
}

wss.on('connection', (ws) => {
  ws.roomCode = null;
  ws.seat = -1;

  ws.on('message', (raw) => {
    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch (_) {
      safeSend(ws, { type: 'error', message: 'Invalid message.' });
      return;
    }

    const type = String(msg.type || '');

    if (type === 'create') {
      leaveRoom(ws);

      const code = makeCode();
      const room = {
        code,
        host: ws,
        players: new Map(),
        rules: null,
        started: false,
      };

      ws.roomCode = code;
      ws.seat = 0;

      room.players.set(0, {
        ws,
        name: String(msg.name || 'HOST').slice(0, 24),
        avatar: msg.avatar || { kind: 'builtin', index: 0 },
        ready: true,
      });

      rooms.set(code, room);

      safeSend(ws, {
        type: 'created',
        code,
        seat: 0,
        players: playerList(room),
      });

      broadcastLobby(room);
      return;
    }

    if (type === 'join') {
      leaveRoom(ws);

      const code = String(msg.code || '');
      const room = rooms.get(code);

      if (!room) {
        safeSend(ws, { type: 'error', message: 'Lobby not found.' });
        return;
      }

      if (room.started) {
        safeSend(ws, { type: 'error', message: 'That match already started.' });
        return;
      }

      const seat = firstOpenSeat(room);

      if (seat < 0) {
        safeSend(ws, { type: 'error', message: 'That lobby is full.' });
        return;
      }

      ws.roomCode = code;
      ws.seat = seat;

      room.players.set(seat, {
        ws,
        name: String(msg.name || `PLAYER ${seat}`).slice(0, 24),
        avatar: msg.avatar || { kind: 'builtin', index: seat },
        ready: false,
      });

      safeSend(ws, {
        type: 'joined',
        code,
        seat,
        players: playerList(room),
      });

      if (room.rules) {
        safeSend(ws, {
          type: 'rules',
          rules: room.rules,
        });
      }

      broadcastLobby(room);
      return;
    }

    const room = roomFor(ws);

    if (!room) {
      safeSend(ws, { type: 'error', message: 'You are not in a lobby.' });
      return;
    }

    if (type === 'ready') {
      const p = room.players.get(ws.seat);
      if (p && ws !== room.host) {
        p.ready = !p.ready;
      }
      broadcastLobby(room);
      return;
    }

    if (type === 'seat_change') {
      const oldSeat = ws.seat;
      const current = room.players.get(oldSeat);
      if (!current) return;

      let target = -1;
      for (let step = 1; step <= 4; step++) {
        const candidate = (oldSeat + step) % 4;
        if (!room.players.has(candidate)) {
          target = candidate;
          break;
        }
      }

      if (target >= 0) {
        room.players.delete(oldSeat);
        room.players.set(target, current);
        ws.seat = target;

        safeSend(ws, {
          type: 'seat_changed',
          seat: target,
        });

        broadcastLobby(room);
      }
      return;
    }

    if (type === 'rules') {
      if (ws !== room.host) return;
      room.rules = msg.rules || {};
      broadcast(room, {
        type: 'rules',
        rules: room.rules,
      }, ws);
      return;
    }

    if (type === 'start') {
      if (ws !== room.host) return;

      for (const p of room.players.values()) {
        if (p.ws !== room.host && !p.ready) {
          safeSend(ws, {
            type: 'error',
            message: 'A connected player is not ready.',
          });
          return;
        }
      }

      room.started = true;
      room.rules = msg.rules || room.rules || {};

      broadcast(room, {
        type: 'start',
        code: room.code,
        players: playerList(room),
        rules: room.rules,
      });
      return;
    }

    if (type === 'action') {
      if (ws === room.host) return;

      safeSend(room.host, {
        type: 'action',
        from: ws.seat,
        action: msg.action || {},
      });
      return;
    }

    if (type === 'state') {
      if (ws !== room.host) return;

      broadcast(room, {
        type: 'state',
        state: msg.state || {},
      }, ws);
      return;
    }

    if (type === 'match_end') {
      if (ws !== room.host) return;

      broadcast(room, {
        type: 'match_end',
        winner: msg.winner,
        title: msg.title,
        scores: msg.scores || [0, 0, 0, 0],
      }, ws);
      return;
    }

    if (type === 'restart_match') {
      if (ws !== room.host) return;
      room.started = true;

      broadcast(room, {
        type: 'restart_match',
      }, ws);
      return;
    }
  });

  ws.on('close', () => leaveRoom(ws));
  ws.on('error', () => leaveRoom(ws));
});

console.log(`UNO Rush relay listening on port ${PORT}`);
