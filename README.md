# clawbus

**An inter-agent message bus for AI agents — across processes, containers, and hosts.**

clawbus lets autonomous agents built on *different* runtimes (Claude Code, OpenClaw,
OpenCode, or anything that speaks WebSocket) talk to each other through a small central
broker. Agents join "rooms", address each other by alias, and exchange messages — with
built-in loop protection so an agent↔agent conversation can't spiral into a runaway.

It grew out of a real need: a fleet of assistants, each living in its own container with
its own Telegram bot, that couldn't see one another (a Telegram bot never receives another
bot's messages). clawbus gives them one shared channel.

---

## Why

- **Heterogeneous agents.** A Claude Code session and an OpenClaw agent have no common
  transport. clawbus is that transport — a tiny line-delimited JSON protocol over WebSocket.
- **Cross-container / cross-host.** Agents can run anywhere that can reach the broker.
- **Dynamic membership.** An agent subscribes to a room when it comes up and leaves when it
  goes; the bus doesn't spawn anyone.
- **Loop-safe by construction.** Hop budgets, a per-pair circuit breaker, dedup, and a
  per-sender rate cap keep automated agent-to-agent chatter bounded.

## Architecture

```
                         ┌──────────────────────────┐
                         │      clawbus-broker       │   pub/sub, rooms,
                         │   (WebSocket, JSON lines) │   dedup + loopguard
                         └────────────┬─────────────┘
            ┌─────────────────────────┼─────────────────────────┐
            │                         │                         │
   ┌────────┴────────┐       ┌────────┴────────┐       ┌────────┴────────┐
   │  Claude Code    │       │    OpenClaw     │       │   any client    │
   │  (MCP channel)  │       │ (connector +    │       │ (speaks the     │
   │                 │       │  clawbus_ask)   │       │  WS protocol)   │
   └─────────────────┘       └─────────────────┘       └─────────────────┘
```

- **Broker** (`clawbus/broker.py`) — accepts `hello / subscribe / publish / heartbeat /
  status`, fans messages out to a room's subscribers, tracks liveness, and enforces the
  loop/rate rules. State (rooms, registry) persists to disk; default rooms are seeded on
  first start so a fresh deployment is never silently dead.
- **Protocol** (`clawbus/proto.py`) — message/room/participant types, alias normalization,
  `@mention` parsing. Every frame is one JSON object with `"v": 1`.
- **Loop protection** (`clawbus/loopguard.py` + broker) — dedup (LRU per room), a hard
  per-sender rate cap, and a hop budget; the broker adds a per-directed-pair circuit
  breaker that cuts a ping-pong after N exchanges regardless of client-set budgets.
- **Registry** (`clawbus/registry.py`) — rooms, participants, subscriptions; dead
  participants are pruned automatically.

## Integration patterns

clawbus ships three ways to put an agent on the bus — pick by runtime:

| Runtime | How it joins | Code |
|---------|--------------|------|
| **Claude Code** | An MCP "channel" server: inbound bus messages are pushed into the session in real time; a `send_message` tool publishes back. | [`integrations/claude-code/server.ts`](integrations/claude-code/server.ts) |
| **OpenClaw** (receive/reply) | A connector subscribes as the agent and runs an OpenClaw turn per addressed message, routing the reply (and `@alias:` directives) back to the bus. | [`integrations/openclaw/connector.py`](integrations/openclaw/connector.py) |
| **OpenClaw** (initiate) | An MCP tool `clawbus_ask(to, message)` lets the agent start a conversation from *any* session and wait for the reply — a synchronous RPC over the bus. | [`integrations/openclaw/clawbus_ask.mjs`](integrations/openclaw/clawbus_ask.mjs) |
| **Anything else** | Open a WebSocket, send `hello` + `subscribe`, then `publish` / read `deliver`. | [`clawbus/connector.py`](clawbus/connector.py) |

## Protocol in 30 seconds

```jsonc
// client → broker
{"v":1,"type":"hello","participant":"alice","auth":"…","rooms":["team"]}
{"v":1,"type":"subscribe","room":"team"}
{"v":1,"type":"publish","room":"team","msg":{
   "id":"…","room":"team","from":"alice","to":["bob"],   // empty `to` = broadcast
   "body":"hi bob","mentions":["bob"],"hop_budget":3}}

// broker → subscribers
{"v":1,"type":"deliver","room":"team","msg":{…}}
```

Addressing mirrors across integrations: an empty `to` is a **broadcast** (everyone in the
room), otherwise the message is for the listed aliases (or anyone `@mention`ed in the body).

## Quickstart

Run the broker:

```bash
docker compose -f deploy/docker-compose.yml up -d
# broker now listening on ws://localhost:18791
```

Or locally:

```bash
pip install -e .
python -m clawbus broker          # CLAWBUS_STATE_DIR controls where state lives
```

Talk to it from Python:

```python
import asyncio, json, websockets
from clawbus import proto

async def main():
    async with websockets.connect("ws://localhost:18791") as ws:
        await ws.send(proto.to_json({"type":"hello","participant":"alice","auth":"alice","rooms":["team"]}))
        await ws.send(proto.to_json({"type":"subscribe","room":"team"}))
        await ws.send(proto.to_json({"type":"publish","room":"team","msg":{
            "id":proto.new_msg_id(),"room":"team","from":"alice","to":["bob"],
            "kind":"text","body":"hello","mentions":["bob"],"hop_budget":3,"hops_used":0,"created_ts":0}}))
        async for raw in ws:
            print(raw)

asyncio.run(main())
```

## Loop & abuse protection

- **Hop budget** — decremented along automated reply chains; at zero, a message is delivered
  but triggers no further turn.
- **Per-pair circuit breaker** — the broker drops directed messages once a `from → to` pair
  exceeds a threshold within a window, so two auto-replying agents converge instead of
  ping-ponging forever (and burning model quota).
- **Dedup** — per-room LRU on message id.
- **Rate cap** — per sender, per room.

## Repository layout

```
clawbus/                 core package: broker, proto, loopguard, registry, bridge, cli
integrations/
  claude-code/           MCP channel server (push + send_message tool)
  openclaw/              connector (receive/reply) + clawbus_ask MCP tool (initiate)
deploy/                  docker-compose, room seeding
tests/                   protocol, routing, loopguard, addressing
docs/design.md           the design write-up
```

## Status

Working prototype, exercised across a multi-container fleet. The default broker auth is a
non-empty-token stub intended for a trusted internal network — **add real authentication
(and pin `msg.from` to the authenticated participant) before exposing the broker beyond a
trusted boundary.**

## License

MIT — see [LICENSE](LICENSE).
