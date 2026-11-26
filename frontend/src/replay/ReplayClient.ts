/**
 * Client for the customer reply stream at /ws/customer/{conversationId}.
 *
 * - Messages are kept in a map keyed by message_id, so a replayed or repeated
 *   message never appears twice.
 * - Each received message is acknowledged (or, in manual mode, on request). The
 *   server stores the Redis cursor only after that ack.
 * - An unexpected close reconnects with exponential backoff and jitter, reusing
 *   the same conversation ID. The server then replays everything after the cursor.
 */

export interface ReplayMessage {
  type: "chat_message";
  conversation_id: string;
  message_id: string;
  sequence_number: number;
  sender_type: string;
  sender_id: string;
  sender_name: string;
  content: string;
  timestamp: string;
  correlation_id: string;
  replayed: boolean;
}

export interface Cursor {
  last_acknowledged_sequence: number;
  last_message_id: string;
  updated_at: string;
}

export type ReplayStatus = "idle" | "connecting" | "replaying" | "connected" | "reconnecting" | "disconnected" | "closed";

export interface ReplayState {
  status: ReplayStatus;
  messages: ReplayMessage[];
  lastReceivedSequence: number;
  cursor: Cursor | null;
  cursorSource: string;
  pod: string;
  reconnected: boolean;
  reconnectAttempt: number;
  reconnections: number;
  lastReplay: { from: number; to: number; count: number } | null;
  duplicatesIgnored: number;
  error: string;
}

export interface SocketLike {
  send(data: string): void;
  close(code?: number, reason?: string): void;
  onopen: ((event: unknown) => void) | null;
  onmessage: ((event: { data: unknown }) => void) | null;
  onclose: ((event: { code: number; reason?: string }) => void) | null;
  onerror: ((event: unknown) => void) | null;
}

export interface ReplayClientOptions {
  url: string;
  token: string;
  conversationId: string;
  autoAck?: boolean;
  onState: (state: ReplayState) => void;
  socketFactory?: (url: string) => SocketLike;
  random?: () => number;
}

const TERMINAL: Record<number, string> = {
  4401: "Your session has expired. Sign in again.",
  4404: "This conversation was not found, or it belongs to someone else.",
};

export function backoffDelay(attempt: number, random: () => number = Math.random): number {
  const ceiling = Math.min(30_000, 500 * 2 ** attempt);
  return ceiling / 2 + random() * (ceiling / 2);
}

export class ReplayClient {
  private socket: SocketLike | null = null;
  private readonly byId = new Map<string, ReplayMessage>();
  private userClosed = true;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private ackRetryTimer: ReturnType<typeof setTimeout> | null = null;
  private pendingAck: ReplayMessage | null = null;
  private autoAck: boolean;
  private readonly random: () => number;
  private everConnected = false;
  private state: ReplayState = {
    status: "idle",
    messages: [],
    lastReceivedSequence: 0,
    cursor: null,
    cursorSource: "",
    pod: "",
    reconnected: false,
    reconnectAttempt: 0,
    reconnections: 0,
    lastReplay: null,
    duplicatesIgnored: 0,
    error: "",
  };

  constructor(private readonly options: ReplayClientOptions) {
    this.autoAck = options.autoAck ?? true;
    this.random = options.random ?? Math.random;
  }

  /** Open the socket. Also used by the Reconnect button after a manual disconnect. */
  connect(): void {
    this.clearTimers();
    this.userClosed = false;
    this.open();
  }

  /** Close on purpose: no automatic reconnect until connect() is called again. */
  disconnect(): void {
    this.userClosed = true;
    this.clearTimers();
    const socket = this.socket;
    this.socket = null;
    socket?.close(1000, "customer disconnected");
    this.update({ status: "disconnected", error: "" });
  }

  setAutoAck(enabled: boolean): void {
    this.autoAck = enabled;
  }

  /** Acknowledge everything up to and including this message. */
  acknowledge(message: ReplayMessage): void {
    this.pendingAck = message;
    this.sendFrame({ type: "ack", sequence_number: message.sequence_number, message_id: message.message_id });
  }

  sendCustomerMessage(content: string): string {
    const clientMessageId = crypto.randomUUID();
    this.sendFrame({ type: "customer_message", client_message_id: clientMessageId, content });
    return clientMessageId;
  }

  get current(): ReplayState {
    return this.state;
  }

  private open(): void {
    const factory = this.options.socketFactory ?? ((url: string) => new WebSocket(url) as unknown as SocketLike);
    this.update({ status: this.everConnected ? "reconnecting" : "connecting" });
    const socket = factory(this.options.url);
    this.socket = socket;
    socket.onopen = () => {
      if (socket !== this.socket) return;
      this.sendFrame({
        type: "hello",
        token: this.options.token,
        // Only used if the server cannot read the Redis cursor.
        last_received_sequence: this.state.lastReceivedSequence,
      });
    };
    socket.onmessage = (event) => {
      if (socket === this.socket) this.onFrame(JSON.parse(String(event.data)));
    };
    socket.onclose = (event) => {
      if (socket === this.socket) this.onClose(event.code, event.reason ?? "");
    };
    socket.onerror = () => undefined;
  }

  private onClose(code: number, reason: string): void {
    this.socket = null;
    if (this.userClosed) return;
    if (TERMINAL[code]) {
      this.userClosed = true;
      this.update({ status: "closed", error: TERMINAL[code] });
      return;
    }
    const attempt = this.state.reconnectAttempt + 1;
    const error =
      code === 1013 || code === 1006 || code === 1011
        ? "The chat server is unavailable. Reconnecting automatically…"
        : code === 1012
          ? "The chat server restarted. Reconnecting to another instance…"
          : reason || "Connection lost. Reconnecting…";
    this.update({ status: "reconnecting", reconnectAttempt: attempt, error });
    this.reconnectTimer = setTimeout(() => this.open(), backoffDelay(attempt - 1, this.random));
  }

  private onFrame(frame: Record<string, any>): void {
    switch (frame.type) {
      case "connection_ack":
        if (this.everConnected) this.state.reconnections += 1;
        this.everConnected = true;
        this.update({
          status: "replaying",
          pod: frame.pod,
          reconnected: frame.reconnected,
          cursor: frame.cursor,
          cursorSource: frame.cursor_source,
          reconnectAttempt: 0,
          error: frame.cursor_source === "redis" || frame.cursor_source === "none" ? "" : "Cursor store unavailable: replaying from the durable log.",
        });
        break;
      case "replay_started":
        this.update({ status: "replaying", lastReplay: { from: frame.from_sequence, to: frame.to_sequence, count: frame.count } });
        break;
      case "replay_completed":
        this.update({ status: "connected" });
        break;
      case "chat_message":
        this.receive(frame as ReplayMessage);
        break;
      case "ack_confirmed":
        this.pendingAck = null;
        this.update({ cursor: frame.cursor });
        break;
      case "customer_message_accepted":
        break;
      case "error":
        this.onError(frame);
        break;
    }
  }

  private receive(message: ReplayMessage): void {
    if (this.byId.has(message.message_id)) {
      this.update({ duplicatesIgnored: this.state.duplicatesIgnored + 1 });
    } else {
      this.byId.set(message.message_id, message);
      const messages = [...this.byId.values()].sort((a, b) => a.sequence_number - b.sequence_number);
      this.update({ messages, lastReceivedSequence: Math.max(this.state.lastReceivedSequence, message.sequence_number) });
    }
    // Acknowledge even a duplicate: this socket did receive it, and the cursor only moves forward.
    if (this.autoAck) this.acknowledge(message);
  }

  private onError(frame: Record<string, any>): void {
    if (frame.code === "cursor_unavailable" && this.pendingAck) {
      const message = this.pendingAck;
      if (this.ackRetryTimer === null) {
        this.ackRetryTimer = setTimeout(() => {
          this.ackRetryTimer = null;
          this.acknowledge(message);
        }, backoffDelay(1, this.random));
      }
    }
    this.update({ error: frame.detail ?? frame.code });
  }

  private sendFrame(frame: object): void {
    try {
      this.socket?.send(JSON.stringify(frame));
    } catch {
      // The socket is closing; onclose takes over.
    }
  }

  private clearTimers(): void {
    if (this.reconnectTimer !== null) clearTimeout(this.reconnectTimer);
    if (this.ackRetryTimer !== null) clearTimeout(this.ackRetryTimer);
    this.reconnectTimer = null;
    this.ackRetryTimer = null;
  }

  private update(patch: Partial<ReplayState>): void {
    this.state = { ...this.state, ...patch };
    this.options.onState(this.state);
  }
}
