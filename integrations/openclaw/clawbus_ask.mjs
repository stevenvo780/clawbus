#!/usr/bin/env node
/* clawbus-mcp — MCP stdio server con la tool `clawbus_ask`: manda un mensaje a otro
 * agente de la flota por el bus clawbus y ESPERA su respuesta (RPC síncrono). Permite
 * que un agente OpenClaw (Dave) INICIE conversaciones por el bus desde cualquier
 * sesión (incluida su Telegram), no solo cuando lo contactan por el bus.
 * Node 22 trae WebSocket global → sin dependencias. Logs a stderr (stdout es MCP). */

const AGENT = process.env.CLAWBUS_AGENT || "openclaw";
const BROKER = process.env.CLAWBUS_BROKER_URL || "ws://clawbus-broker:18791";
const ROOM = process.env.CLAWBUS_ROOM || "agentes";

const log = (...a) => process.stderr.write("[clawbus-mcp] " + a.join(" ") + "\n");

function newId() {
  let s = "", h = "0123456789abcdef";
  for (let i = 0; i < 24; i++) s += h[Math.floor(Math.random() * 16)];
  return s;
}
const v1 = (obj) => JSON.stringify({ v: 1, ...obj });

function clawbusAsk(to, message, waitSeconds) {
  return new Promise((resolve) => {
    const alias = AGENT + "-ask-" + newId().slice(0, 8);  // alias ÚNICO por llamada
    // (no choca con el connector ni entre asks concurrentes que pisarían su conexión)
    let done = false, ws;
    const finish = (txt) => { if (done) return; done = true; clearTimeout(timer);
      try { ws && ws.close(); } catch {} resolve(txt); };
    const timer = setTimeout(
      () => finish(`(sin respuesta de ${to} en ${waitSeconds}s — puede estar ocupado o dormido)`),
      waitSeconds * 1000);
    try { ws = new WebSocket(BROKER); }
    catch (e) { finish(`(error conectando al bus: ${e})`); return; }
    ws.onopen = () => {
      ws.send(v1({ type: "hello", participant: alias, auth: alias, rooms: [ROOM] }));
      ws.send(v1({ type: "subscribe", room: ROOM }));
      const msg = { id: newId(), room: ROOM, from: alias, to: [to], kind: "text",
        body: `[${AGENT}] ${message}`, mentions: [to], in_reply_to: null,
        hop_budget: 3, hops_used: 0, created_ts: 0 };
      ws.send(v1({ type: "publish", room: ROOM, msg }));
      log(`ask -> ${to}: ${message.slice(0, 60)}`);
    };
    ws.onmessage = (ev) => {
      const data = typeof ev.data === "string" ? ev.data : String(ev.data);
      for (const line of data.split("\n")) {
        const t = line.trim(); if (!t) continue;
        let e; try { e = JSON.parse(t); } catch { continue; }
        if (e.type !== "deliver") continue;
        const m = e.msg || {};
        if (m.from === to) { finish(m.body || "(respuesta vacía)"); return; }
      }
    };
    ws.onerror = (e) => finish(`(error de bus: ${e && e.message ? e.message : e})`);
  });
}

const TOOL = {
  name: "clawbus_ask",
  description: "Enviar un mensaje a otro agente de la flota por el bus clawbus (ej: " +
    "carol, frank, grace, erin) y ESPERAR su respuesta. Usalo cuando te pidan " +
    "contactar, preguntarle o avisarle algo a otro agente; devuelve lo que ese agente conteste.",
  inputSchema: {
    type: "object",
    properties: {
      to: { type: "string", description: "alias del agente destino (carol, frank, grace, erin, ...)" },
      message: { type: "string", description: "el mensaje para ese agente" },
      wait_seconds: { type: "number", description: "segundos a esperar la respuesta (default 90)" },
    },
    required: ["to", "message"],
  },
};

const send = (obj) => process.stdout.write(JSON.stringify(obj) + "\n");

async function handle(req) {
  const { id, method } = req;
  const params = req.params || {};
  if (method === "initialize") {
    send({ jsonrpc: "2.0", id, result: {
      protocolVersion: params.protocolVersion || "2025-06-18",
      capabilities: { tools: {} },
      serverInfo: { name: "clawbus-mcp", version: "1.0.0" } } });
  } else if (method === "ping") {
    send({ jsonrpc: "2.0", id, result: {} });
  } else if (method === "notifications/initialized") {
    /* sin respuesta */
  } else if (method === "tools/list") {
    send({ jsonrpc: "2.0", id, result: { tools: [TOOL] } });
  } else if (method === "tools/call") {
    if (params.name !== "clawbus_ask") {
      send({ jsonrpc: "2.0", id, error: { code: -32601, message: "unknown tool" } }); return;
    }
    const a = params.arguments || {};
    const reply = await clawbusAsk(String(a.to || ""), String(a.message || ""),
      Number(a.wait_seconds) > 0 ? Number(a.wait_seconds) : 90);
    send({ jsonrpc: "2.0", id, result: { content: [{ type: "text", text: reply }] } });
  } else if (id !== undefined) {
    send({ jsonrpc: "2.0", id, error: { code: -32601, message: "method not found: " + method } });
  }
}

let buf = "";
process.stdin.on("data", (chunk) => {
  buf += chunk.toString();
  let idx;
  while ((idx = buf.indexOf("\n")) >= 0) {
    const line = buf.slice(0, idx).trim();
    buf = buf.slice(idx + 1);
    if (!line) continue;
    let req; try { req = JSON.parse(line); } catch { continue; }
    handle(req);
  }
});

log(`STARTED agent=${AGENT} broker=${BROKER} room=${ROOM}`);
