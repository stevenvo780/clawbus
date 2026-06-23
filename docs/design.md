# clawbus — design notes

## Problem

Autonomous agents increasingly run as long-lived processes — a coding agent in one
container, a chat assistant in another, a task runner on a third host. They have no shared
transport. Worse, common chat backends actively prevent peer discovery: a Telegram bot, for
example, never receives another bot's messages, so two bot-fronted agents in the same group
are blind to each other.

clawbus is a small, explicit message bus that gives heterogeneous agents one channel to
talk on, while keeping automated agent-to-agent traffic from running away.

## Goals

- **Runtime-agnostic.** Anything that speaks WebSocket + a tiny JSON protocol can join.
- **Dynamic membership.** Agents subscribe when they start and drop when they stop; the bus
  never spawns participants.
- **Room isolation.** Traffic is scoped to rooms; a participant only sees rooms it joined.
- **Loop-safe.** Automated reply chains are bounded structurally, not by convention.
- **Small.** A single broker process, file-backed state, no external dependencies beyond a
  WebSocket library.

## Protocol

Line-delimited JSON over WebSocket. Every frame is one object carrying `"v": 1`.

### Client → broker

| type | fields | meaning |
|------|--------|---------|
| `hello` | `participant`, `auth`, `rooms[]` | authenticate + announce |
| `subscribe` / `unsubscribe` | `room` | join / leave a room |
| `publish` | `room`, `msg` | send a message (see below) |
| `heartbeat` | `participant`, `state`, `quota` | liveness + load |
| `status` | — | request the live roster |

### Broker → client

| type | fields | meaning |
|------|--------|---------|
| `deliver` | `room`, `msg` | a message for a room you're in |
| `ack` | `status` | result of your publish (`delivered`, `dedup_hit`, `dropped`, `loop_suspected`, …) |
| `status` | `rooms`, `participants[]` | the roster |
| `nudge` | `room`, `to`, `reason` | broker poking an idle participant |
| `error` | `message` | malformed / rejected frame |

### Message

```jsonc
{
  "id": "…",            // unique id (dedup key)
  "room": "team",
  "from": "alice",
  "to": ["bob"],        // empty/absent  =>  broadcast to the room
  "kind": "text",
  "body": "hello bob",
  "mentions": ["bob"],  // parsed from @mentions in the body
  "in_reply_to": null,
  "hop_budget": 3,      // remaining automated hops
  "hops_used": 0,
  "created_ts": 0
}
```

**Addressing.** Empty `to` is a broadcast — every subscriber receives it. Otherwise the
message is *for* the listed aliases; integrations also treat an `@mention` in the body as
addressing. This rule is mirrored on both the Claude Code channel and the OpenClaw
connector so behavior is identical regardless of runtime.

**Alias normalization.** Aliases are matched after NFKC + case-fold + diacritic stripping,
so `@Álex`, `alex`, and `ALEX` resolve to one identity.

## Broker

- **Fan-out.** On `publish`, the broker validates (below) then delivers to every current
  subscriber of the room. Delivery is room-wide; `to`/`mentions` are addressing metadata the
  receiver acts on.
- **State.** Rooms + participants + subscriptions persist to a state directory (atomic
  rename on write). Default rooms are seeded on first start, so a fresh deployment is never
  silently roomless.
- **Liveness.** Heartbeats move a participant alive -> idle -> dead; long-dead participants
  are pruned so the registry and fan-out don't grow without bound.

## Loop & abuse protection

Automated agents replying to each other can loop. clawbus defends in layers:

1. **Dedup** — per-room LRU on `msg.id`; a replayed id is dropped.
2. **Hop budget** — carried on the message and decremented along automated reply chains. At
   zero, a message is still delivered but must not trigger another turn. Connectors derive a
   reply's budget from the inbound message.
3. **Per-pair circuit breaker** — the broker counts directed `from -> to` messages in a
   sliding window and drops (`loop_suspected`) once a pair exceeds a threshold. This bounds a
   ping-pong *structurally*, even if a client resets `hop_budget` on every send — which is the
   failure mode a budget-only scheme misses.
4. **Per-sender rate cap** — a hard ceiling on messages per sender per room per minute.

The circuit breaker is the load-bearing one: a budget that the sender controls is advisory;
a pair counter the broker controls is not.

## Integration patterns

- **Claude Code (MCP channel).** A channel server connects to the broker; inbound bus
  messages are *pushed* into the session in real time, and a `send_message(to, body)` tool
  publishes back. The agent experiences siblings as just another channel.
- **OpenClaw (connector).** A bridge subscribes as the agent, runs one agent turn per
  addressed/broadcast message, and routes the reply back. The turn prompt is enriched with
  the live roster and an `@alias: …` convention, so the agent can also *direct* a message to
  a specific sibling, which the connector forwards.
- **OpenClaw (initiate).** Because the connector only reacts to inbound traffic, a separate
  MCP tool `clawbus_ask(to, message)` lets an agent *start* a conversation from any session:
  it publishes under a unique ephemeral alias and waits for the reply — a synchronous RPC
  over the bus.

## Security note

The reference broker requires a non-empty `auth` token but does not verify identity or pin
`msg.from` to the authenticated participant. That is acceptable only on a trusted internal
network. Before exposing the broker across a trust boundary: authenticate participants, pin
`from` to the authenticated alias, and scope room access. These are deliberately left as
integration points rather than baked-in policy.
