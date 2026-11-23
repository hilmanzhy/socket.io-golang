// Scenario run by cluster_test.go: a Node.js server and a socket.io-golang
// server share one Redis, and each must apply what the other broadcasts.
// Usage: node cluster.mjs <url of the Go server> <redis host:port> [key]
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { createAdapter } from "@socket.io/redis-adapter";
import { Redis } from "ioredis";
import { Server } from "socket.io";
import { io as connect } from "socket.io-client";

const [goURL, redisAddress, key] = process.argv.slice(2);
const [redisHost, redisPort] = redisAddress.split(":");

const watchdog = setTimeout(() => {
  console.error("not ok - scenario timed out");
  process.exit(1);
}, 40_000);

const once = (emitter, event) =>
  new Promise((resolve) => emitter.once(event, (...args) => resolve(args)));

const request = (target, event, ...args) =>
  new Promise((resolve) => target.emit(event, ...args, (...answer) => resolve(answer)));

async function step(name, run) {
  await run();
  console.log(`ok - ${name}`);
}

const publisher = new Redis(Number(redisPort), redisHost);
const subscriber = publisher.duplicate();
const httpServer = createServer();
const io = new Server(httpServer, {
  adapter: createAdapter(publisher, subscriber, key ? { key } : undefined),
});
io.on("connection", (socket) => {
  socket.on("enter", (room, done) => {
    socket.join(room);
    done(true);
  });
});
await new Promise((resolve) => httpServer.listen(0, "127.0.0.1", resolve));
const nodeURL = `http://127.0.0.1:${httpServer.address().port}`;

const open = async (url) => {
  const socket = connect(url, { transports: ["websocket"], reconnection: false, forceNew: true });
  await once(socket, "connect");
  return socket;
};

const goMember = await open(goURL);
const goOutsider = await open(goURL);
const nodeMember = await open(nodeURL);
const nodeOutsider = await open(nodeURL);
const everyone = [goMember, goOutsider, nodeMember, nodeOutsider];

// applied runs a join or a leave on the Node.js server and waits for it to
// take effect there: that server applies its own request only once Redis has
// sent it back, and its adapter listens to the subscriber before this does.
const applied = async (run) => {
  const returned = once(subscriber, "messageBuffer");
  run();
  await returned;
};

// The two servers behind one interface: node runs an operation on the Node.js
// server, go asks the Go server to run it.
const node = {
  name: "Node.js",
  emit: (rooms, except, event, ...args) => io.to(rooms).except(except).emit(event, ...args),
  join: (targets, rooms) => applied(() => io.in(targets).socketsJoin(rooms)),
  leave: (targets, rooms) => applied(() => io.in(targets).socketsLeave(rooms)),
  kick: async (targets, close) => io.in(targets).disconnectSockets(close),
};
const go = {
  name: "Go",
  emit: (rooms, except, event, ...args) => goOutsider.emit("emit", rooms, except, event, ...args),
  join: (targets, rooms) => request(goOutsider, "join", targets, rooms),
  leave: (targets, rooms) => request(goOutsider, "leave", targets, rooms),
  kick: (targets, close) => request(goOutsider, "kick", targets, close),
};

let sequence = 0;

// deliver sends an event from origin and checks who receives it: every socket
// of receivers and no other. A second event that reaches every socket follows
// it on the same path, so that a stray delivery has had the time to arrive.
// It returns the arguments each receiver got.
async function deliver(origin, { rooms = [], except = [] }, receivers, args = []) {
  const event = `event-${++sequence}`;
  const marker = `marker-${sequence}`;
  const strays = [];
  for (const socket of everyone) {
    if (!receivers.includes(socket)) {
      socket.once(event, () => strays.push(socket.id));
    }
  }
  const arrivals = receivers.map((socket) => once(socket, event));
  const settled = everyone.map((socket) => once(socket, marker));
  origin.emit(rooms, except, event, ...args);
  origin.emit([], [], marker);
  const received = await Promise.all(arrivals);
  await Promise.all(settled);
  assert.deepEqual(strays, []);
  return received;
}

// until repeats attempt until event reaches socket: a server subscribes to
// Redis in the background, so its first publications may be missed.
async function until(socket, event, attempt) {
  const arrived = once(socket, event);
  const timer = setInterval(attempt, 50);
  attempt();
  await arrived;
  clearInterval(timer);
}

await step("each server receives the broadcasts of the other", async () => {
  await until(goMember, "warm-up-node", () => node.emit([], [], "warm-up-node"));
  await until(nodeMember, "warm-up-go", () => go.emit([], [], "warm-up-go"));
});

const blob = Buffer.from([0, 1, 2, 254, 255]);
const plain = { text: "héllo ✓ <b>&</b>", list: [1, 2.5, -3, null, true, 2 ** 40], empty: {} };

for (const origin of [node, go]) {
  await step(`a broadcast from ${origin.name} reaches every socket with its arguments`, async () => {
    const sent = ["text", 42, plain, blob];
    for (const received of await deliver(origin, {}, everyone, sent)) {
      assert.deepEqual(received, sent);
    }
  });
}

await step("a binary value nested in an argument of Node.js reaches a socket of Go", async () => {
  const [[received]] = await deliver(node, { rooms: [goMember.id] }, [goMember], [{ file: blob }]);
  assert.deepEqual(received, { file: blob });
});

await step("sockets enter a room on their own server", async () => {
  assert.deepEqual(await request(goMember, "enter", "club"), [true]);
  assert.deepEqual(await request(nodeMember, "enter", "club"), [true]);
});

for (const origin of [node, go]) {
  await step(`a room broadcast from ${origin.name} reaches the members on both servers`, async () => {
    await deliver(origin, { rooms: ["club"] }, [goMember, nodeMember]);
    await deliver(origin, { rooms: ["club", "nowhere"] }, [goMember, nodeMember]);
  });

  await step(`a broadcast from ${origin.name} skips the excluded rooms on both servers`, async () => {
    await deliver(origin, { except: ["club"] }, [goOutsider, nodeOutsider]);
    await deliver(origin, { rooms: ["club"], except: [goMember.id] }, [nodeMember]);
    await deliver(origin, { rooms: ["club"], except: [nodeMember.id] }, [goMember]);
  });

  await step(`a join and a leave from ${origin.name} move the sockets of both servers`, async () => {
    const room = `lounge of ${origin.name}`;
    await origin.join(["club"], [room]);
    await deliver(origin, { rooms: [room] }, [goMember, nodeMember]);
    await origin.leave([goMember.id, nodeMember.id], [room]);
    await deliver(origin, { rooms: [room] }, []);
  });
}

await step("Go ignores the requests it does not implement", async () => {
  const sockets = await io.fetchSockets();
  assert.equal(sockets.length, io.of("/").sockets.size);
  io.serverSideEmit("hello", "servers");
  await new Promise((resolve) => io.timeout(200).emit("acknowledge", () => resolve()));
  await deliver(node, {}, everyone);
  await deliver(go, {}, everyone);
});

for (const [origin, url] of [[node, goURL], [go, nodeURL]]) {
  await step(`a disconnect from ${origin.name} reaches a socket of the other server`, async () => {
    for (const close of [false, true]) {
      const victim = await open(url);
      const disconnected = once(victim, "disconnect");
      await origin.kick([victim.id], close);
      assert.equal((await disconnected)[0], "io server disconnect");
    }
    await deliver(origin, {}, everyone);
  });
}

for (const socket of everyone) {
  socket.disconnect();
}
await io.close();
await Promise.all([publisher.quit(), subscriber.quit()]);
clearTimeout(watchdog);
