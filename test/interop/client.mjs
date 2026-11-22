// Scenario run by interop_test.go against a socket.io-golang server.
// Usage: node client.mjs <url> <transports, comma separated>
import assert from "node:assert/strict";
import { Manager } from "socket.io-client";

const [url, transportList] = process.argv.slice(2);
const transports = transportList.split(",");

const watchdog = setTimeout(() => {
  console.error("not ok - scenario timed out");
  process.exit(1);
}, 20_000);

const once = (emitter, event) =>
  new Promise((resolve) => emitter.once(event, (...args) => resolve(args)));

const request = (target, event, ...args) =>
  new Promise((resolve) => target.emit(event, ...args, (...answer) => resolve(answer)));

const newManager = () => new Manager(url, { transports, reconnection: false });

async function step(name, run) {
  await run();
  console.log(`ok - ${name}`);
}

const manager = newManager();
const upgraded = transports.length > 1 ? once(manager.engine, "upgrade") : null;
const socket = manager.socket("/");
const greeting = once(socket, "hello");

await step("connects to the main namespace", async () => {
  await once(socket, "connect");
  assert.equal(typeof socket.id, "string");
  const [id] = await greeting;
  assert.equal(id, socket.id);
});

if (upgraded) {
  await step("upgrades from polling to websocket", async () => {
    await upgraded;
    assert.equal(manager.engine.transport.name, "websocket");
  });
}

await step("acknowledges an event with its arguments", async () => {
  const sent = ["text", 42, { nested: [1, 2, null], accent: "héllo ✓" }];
  const received = await request(socket, "echo", ...sent);
  assert.deepEqual(received, sent);
});

await step("carries binary arguments both ways", async () => {
  const blob = Buffer.from([0, 1, 2, 254, 255]);
  const [echoed, tail] = await request(socket, "echo", blob, "tail");
  assert.ok(Buffer.isBuffer(echoed));
  assert.deepEqual(echoed, blob);
  assert.equal(tail, "tail");
});

await step("inlines a nested binary value as base64", async () => {
  const blob = Buffer.from("nested");
  const [echoed] = await request(socket, "echo", { file: blob });
  assert.deepEqual(echoed, { file: blob.toString("base64") });
});

await step("carries a large payload", async () => {
  const large = "x".repeat(300_000);
  const [echoed] = await request(socket, "echo", large);
  assert.equal(echoed, large);
});

await step("keeps a burst of events in order", async () => {
  const numbers = Array.from({ length: 200 }, (_, i) => i);
  const answers = await Promise.all(numbers.map((n) => request(socket, "echo", n)));
  assert.deepEqual(answers.flat(), numbers);
});

await step("answers an acknowledgement requested by the server", async () => {
  socket.on("question", (subject, reply) => reply(`node:${subject}`));
  const answered = once(socket, "answered");
  socket.emit("ask", "name");
  assert.deepEqual(await answered, ["node:name"]);
});

const peerManager = newManager();
const peer = peerManager.socket("/");
await once(peer, "connect");

await step("broadcasts to a room", async () => {
  const rooms = await request(socket, "join", "blue");
  assert.deepEqual(rooms, [[socket.id, "blue"].sort()]);
  const shouted = once(socket, "shouted");
  let leaked = false;
  peer.on("shouted", () => (leaked = true));
  peer.emit("shout", "blue", "hi");
  assert.deepEqual(await shouted, ["hi"]);
  await request(peer, "echo");
  assert.equal(leaked, false);
});

await step("skips the sender of a socket broadcast", async () => {
  await request(peer, "join", "blue");
  const whispered = once(socket, "whispered");
  let leaked = false;
  peer.on("whispered", () => (leaked = true));
  peer.emit("whisper", "blue", "psst");
  assert.deepEqual(await whispered, ["psst"]);
  await request(peer, "echo");
  assert.equal(leaked, false);
});

await step("broadcasts binary to every socket", async () => {
  const received = [once(socket, "blob"), once(peer, "blob")];
  socket.emit("blast");
  for (const [blob, label] of await Promise.all(received)) {
    assert.deepEqual(blob, Buffer.from([1, 2, 3]));
    assert.equal(label, "blob");
  }
});

await step("refuses a namespace through its middleware", async () => {
  const denied = manager.socket("/admin", { auth: { token: "wrong" } });
  const [error] = await once(denied, "connect_error");
  assert.equal(error.message, "unauthorized");
  assert.deepEqual(error.data, { code: 401 });
  denied.disconnect();
});

await step("refuses an unknown namespace", async () => {
  const unknown = manager.socket("/nowhere");
  const [error] = await once(unknown, "connect_error");
  assert.equal(error.message, "Invalid namespace");
  unknown.disconnect();
});

const adminManager = newManager();
const adminMain = adminManager.socket("/");
const admin = adminManager.socket("/admin", { auth: { token: "secret" } });

await step("multiplexes two namespaces over one connection", async () => {
  const [[role]] = await Promise.all([once(admin, "welcome"), once(adminMain, "connect")]);
  assert.equal(role, "admin");
  assert.notEqual(admin.id, adminMain.id);
});

await step("reports a server namespace disconnect", async () => {
  const disconnected = once(admin, "disconnect");
  admin.emit("kick");
  assert.equal((await disconnected)[0], "io server disconnect");
  assert.deepEqual(await request(adminMain, "echo", "still here"), ["still here"]);
});

await step("reports a connection closed by the server", async () => {
  const disconnected = once(peer, "disconnect");
  const closed = once(peerManager, "close");
  peer.emit("close");
  assert.equal((await disconnected)[0], "io server disconnect");
  await closed;
});

await step("leaves a namespace, then drops a connection", async () => {
  const closed = once(manager, "close");
  socket.disconnect();
  await closed;
  adminManager.engine.close();
});

clearTimeout(watchdog);
