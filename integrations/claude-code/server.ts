#!/usr/bin/env bun
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';
import { randomUUID } from 'crypto';
import WebSocket from 'ws';
import { appendFileSync } from 'fs';

// Configuration
const CLAWBUS_AGENT = process.env.CLAWBUS_AGENT;
const CLAWBUS_ROOM = process.env.CLAWBUS_ROOM;
const CLAWBUS_BROKER_URL = process.env.CLAWBUS_BROKER_URL ?? 'ws://127.0.0.1:18791';

// Sin CLAWBUS_AGENT/ROOM corre en modo IDLE (MCP cargado, sin conectar al bus).
// Así es seguro registrarlo global: solo se engancha al bus en sesiones de identidad.
const BUS_ENABLED = Boolean(CLAWBUS_AGENT && CLAWBUS_ROOM);

const LOGFILE = `${process.env.HOME || '/tmp'}/.clawbus/channel-${process.env.CLAWBUS_AGENT || 'noagent'}.log`;
function log(msg: string) {
  const line = `[clawbus-channel] ${msg}\n`;
  process.stderr.write(line);
  try { appendFileSync(LOGFILE, new Date().toISOString() + ' ' + line); } catch {}
}
log(`STARTED agent=${process.env.CLAWBUS_AGENT} room=${process.env.CLAWBUS_ROOM} broker=${process.env.CLAWBUS_BROKER_URL}`);

// Inbound message interface matching proto.py
interface ClawbusMessage {
  id: string;
  room: string;
  from: string;
  to: string[];
  kind: string;
  body: string;
  mentions: string[];
  in_reply_to: string | null;
  hop_budget: number;
  hops_used: number;
  created_ts: number;
}

// Buffers
let receivedMessages: ClawbusMessage[] = [];
const outboundQueue: string[] = [];
let current_state = 'alive';
let connected = false;
let ws: WebSocket | null = null;
let reconnectTimeout: any = null;

// Helper to parse mentions from body
function parseMentions(text: string): string[] {
  if (!text) return [];
  const matches = text.match(/@([a-z0-9_-]{1,32})/gi);
  if (!matches) return [];
  const seen = new Set<string>();
  const res: string[] = [];
  for (const m of matches) {
    const name = m.slice(1).toLowerCase().trim();
    if (name && !seen.has(name)) {
      seen.add(name);
      res.push(name);
    }
  }
  return res;
}

// Enqueue event to WebSocket
function enqueue(event: any): void {
  const line = JSON.stringify(event) + '\n';
  if (ws && ws.readyState === WebSocket.OPEN) {
    try {
      ws.send(line);
    } catch (err) {
      log(`Failed to send message: ${err}`);
      outboundQueue.push(line);
    }
  } else {
    outboundQueue.push(line);
    log(`WS not connected, queued event of type ${event.type}`);
  }
}

// Flush outbound queue
function flushQueue(): void {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  while (outboundQueue.length > 0) {
    const line = outboundQueue.shift();
    if (line) {
      try {
        ws.send(line);
      } catch (err) {
        log(`Failed to flush line: ${err}`);
        outboundQueue.unshift(line);
        break;
      }
    }
  }
}

// Handle incoming events from the broker
function handleBrokerEvent(event: any): void {
  const type = event.type;
  if (type === 'ack') {
    return;
  }
  if (type === 'error') {
    log(`Broker error: ${event.message}`);
    return;
  }
  if (type === 'deliver') {
    const msg = event.msg as ClawbusMessage;
    if (!msg) {
      log('Deliver event without msg');
      return;
    }
    // Filter out our own messages
    if (msg.from === CLAWBUS_AGENT) {
      return;
    }
    // Check if message is addressed to us or room broadcast
    const isToUs = !msg.to || msg.to.length === 0 || msg.to.includes(CLAWBUS_AGENT);
    const mentionsUs = msg.mentions && msg.mentions.includes(CLAWBUS_AGENT);

    if (isToUs || mentionsUs) {
      receivedMessages.push(msg);
      log(`Inbound from ${msg.from} (${msg.id}) -> push to session`);
      // PUSH al inbound de la sesión Claude vía el protocolo claude/channel.
      server.notification({
        method: 'notifications/claude/channel',
        params: {
          content: msg.body,
          meta: {
            chat_id: msg.room,
            message_id: msg.id,
            user: msg.from,
            user_id: msg.from,
            ts: new Date(msg.created_ts * 1000).toISOString(),
          },
        },
      }).catch((err: any) => log(`failed to deliver inbound to session: ${err}`));
    }
    return;
  }
  log(`Unhandled event type: ${type}`);
}

// Connect to broker
function connect(): void {
  if (ws) {
    try {
      ws.close();
    } catch {}
    ws = null;
  }

  log(`Connecting to broker at ${CLAWBUS_BROKER_URL}...`);
  ws = new WebSocket(CLAWBUS_BROKER_URL);

  ws.on('open', () => {
    log('WebSocket connected. Registering agent...');
    connected = true;

    // Send hello (auth no-vacío: el broker lo exige; stub acepta cualquiera)
    enqueue({
      v: 1,
      type: 'hello',
      participant: CLAWBUS_AGENT,
      backend: 'claude_code',
      rooms: [CLAWBUS_ROOM],
      auth: process.env.CLAWBUS_AUTH || CLAWBUS_AGENT
    });

    // Subscribe to room
    enqueue({
      v: 1,
      type: 'subscribe',
      room: CLAWBUS_ROOM
    });

    flushQueue();
  });

  ws.on('message', (data) => {
    const text = data.toString();
    const lines = text.split('\n');
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        const event = JSON.parse(trimmed);
        handleBrokerEvent(event);
      } catch (err) {
        log(`Failed to parse broker event: ${err}`);
      }
    }
  });

  ws.on('close', (code, reason) => {
    log(`WebSocket closed: code=${code}, reason=${reason.toString() || 'none'}. Reconnecting...`);
    connected = false;
    scheduleReconnect();
  });

  ws.on('error', (err) => {
    log(`WebSocket error: ${err.message}`);
  });
}

function scheduleReconnect(): void {
  if (reconnectTimeout) return;
  reconnectTimeout = setTimeout(() => {
    reconnectTimeout = null;
    connect();
  }, 5000);
}

// Send periodic heartbeat to keep connection alive
setInterval(() => {
  if (connected) {
    enqueue({
      v: 1,
      type: 'heartbeat',
      state: current_state,
      quota: {}
    });
  }
}, 15000);

// Initialize MCP Server
const server = new Server(
  {
    name: 'clawbus',
    version: '1.0.0',
  },
  {
    capabilities: {
      tools: {},
      experimental: {
        'claude/channel': {},
      },
    },
    instructions: [
      `Sos "${CLAWBUS_AGENT}", un agente conectado al bus clawbus (room "${CLAWBUS_ROOM}").`,
      'Los mensajes de tus hermanos llegan como <channel source="clawbus" chat_id="<room>" user="<remitente>" ...>cuerpo</channel>.',
      'Para delegar o responderle a un hermano usá la tool send_message (to: ["<alias>"], body: "..."). Para hablarle a todos, omití "to".',
      'Solo actuás si te dirigen el mensaje (te mencionan o sos el destinatario). No respondas a lo que no es para vos.',
    ].join('\n'),
  }
);

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: 'send_message',
        description: 'Send a message to one or more participants in the current room, or broadcast it to everyone.',
        inputSchema: {
          type: 'object',
          properties: {
            body: {
              type: 'string',
              description: 'The message body/content to send'
            },
            to: {
              type: 'array',
              items: {
                type: 'string'
              },
              description: 'Array of recipient agent IDs. Leave empty/omit to broadcast to all subscribers in the room.'
            },
            kind: {
              type: 'string',
              description: 'The type of message (e.g. "text", "file", etc.)',
              default: 'text'
            }
          },
          required: ['body']
        }
      },
      {
        name: 'receive_messages',
        description: 'Poll/retrieve and clear the list of recent messages received from the clawbus broker since the last poll.',
        inputSchema: {
          type: 'object',
          properties: {}
        }
      },
      {
        name: 'status',
        description: 'Get connection and agent state, and optionally update the agent state.',
        inputSchema: {
          type: 'object',
          properties: {
            state: {
              type: 'string',
              enum: ['alive', 'working', 'idle', 'dead'],
              description: 'Set a new state for the agent (e.g., "alive", "working", "idle", "dead")'
            }
          }
        }
      }
    ]
  };
});

server.setRequestHandler(CallToolRequestSchema, async (req) => {
  const name = req.params.name;
  const args = (req.params.arguments ?? {}) as any;

  try {
    switch (name) {
      case 'send_message': {
        const body = String(args.body ?? '');
        const to = Array.isArray(args.to) ? args.to.map(String) : [];
        const kind = String(args.kind ?? 'text');

        if (!body) {
          return {
            content: [{ type: 'text', text: 'Error: body is required' }],
            isError: true,
          };
        }

        const msgId = randomUUID().replace(/-/g, '').slice(0, 24);
        const parsedMentions = parseMentions(body);
        const mentions = Array.from(new Set([...to, ...parsedMentions]));

        const messagePayload = {
          id: msgId,
          room: CLAWBUS_ROOM,
          from: CLAWBUS_AGENT,
          to: to,
          kind: kind,
          body: body,
          mentions: mentions,
          in_reply_to: null,
          hop_budget: 3,
          hops_used: 0,
          created_ts: Date.now() / 1000,
        };

        const publishEvent = {
          v: 1,
          type: 'publish',
          room: CLAWBUS_ROOM,
          msg: messagePayload
        };

        enqueue(publishEvent);

        return {
          content: [
            {
              type: 'text',
              text: `Message published successfully. ID: ${msgId}`,
            },
          ],
        };
      }

      case 'receive_messages': {
        const msgs = [...receivedMessages];
        receivedMessages = [];
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify(msgs, null, 2),
            },
          ],
        };
      }

      case 'status': {
        const newState = args.state ? String(args.state).trim() : null;
        if (newState) {
          if (['alive', 'working', 'idle', 'dead'].includes(newState)) {
            current_state = newState;
            enqueue({
              v: 1,
              type: 'heartbeat',
              state: current_state,
              quota: {}
            });
            log(`Agent state updated to: ${current_state}`);
          } else {
            return {
              content: [{ type: 'text', text: 'Error: invalid state. Must be alive, working, idle, or dead.' }],
              isError: true
            };
          }
        }

        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                agentId: CLAWBUS_AGENT,
                room: CLAWBUS_ROOM,
                brokerUrl: CLAWBUS_BROKER_URL,
                connected: connected,
                state: current_state,
                bufferedMessages: receivedMessages.length
              }, null, 2),
            },
          ],
        };
      }

      default:
        return {
          content: [{ type: 'text', text: `Unknown tool: ${name}` }],
          isError: true,
        };
    }
  } catch (err: any) {
    log(`Tool call failed: ${err}`);
    return {
      content: [{ type: 'text', text: `Error: ${err.message || err}` }],
      isError: true,
    };
  }
});

// Handle lifecycle hooks and exit signals to close connection
async function shutdown(signal: string): Promise<void> {
  log(`Received ${signal}, shutting down...`);
  if (reconnectTimeout) {
    clearTimeout(reconnectTimeout);
  }
  if (ws && ws.readyState === WebSocket.OPEN) {
    try {
      ws.close();
    } catch {}
  }
  process.exit(0);
}

process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));

process.on('uncaughtException', (err) => {
  process.stderr.write(`[clawbus-channel] Uncaught exception: ${err.stack || err}\n`);
});

process.on('unhandledRejection', (reason) => {
  process.stderr.write(`[clawbus-channel] Unhandled rejection: ${reason}\n`);
});

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  log('MCP server connected on stdio.');
  if (BUS_ENABLED) connect();
  else log('IDLE: sin CLAWBUS_AGENT/ROOM; cargado pero sin conectar al bus.');
}

main().catch((err) => {
  log(`Error in main: ${err}`);
  process.exit(1);
});
