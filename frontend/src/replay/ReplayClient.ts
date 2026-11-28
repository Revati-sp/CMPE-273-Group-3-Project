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
  session_version?: number;
  updated_at: string;
}

export type ReplayStatus =
  | "idle"
  | "connecting"
  | "handover"
  | "replaying"
  | "connected"
  | "reconnecting"
  | "disconnected"
  | "closed";

export interface Handover {
  phase: "started" | "completed";
  oldPod: string;
  newPod: string;
  reason: string;
  replayedFrom?: number;
  replayedTo?: number;
  replayedCount?: number;
  serverRecoveryMs?: number;
}

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
  sessionVersion: number;
  handover: Handover | null;
  replayedTotal: number;
  gapsDetected: number;
  recoveryMs: number | null;
  workflowState: string;
  agent: string;
  typingStatus: string;
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
  heartbeatMs?: number;
  gapTimeoutMs?: number;
  now?: () => number;
}

const TERMINAL: Record<number, string> = {
  4401: "Your session has expired. Sign in again.",
  4404: "This conversation was not found, or it belongs to someone else.",
  4409: "This conversation was opened in another window or tab.",
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
  private readonly now: () => number;
  private readonly heartbeatMs: number;
  private readonly gapTimeoutMs: number;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private gapTimer: ReturnType<typeof setTimeout> | null = null;
  private awaitingPong = false;
  private lostAt: number | null = null;
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
    sessionVersion: 0,
    handover: null,
    replayedTotal: 0,
    gapsDetected: 0,
    recoveryMs: null,
    workflowState: "",
    agent: "",
    typingStatus: "",
  };

  constructor(private readonly options: ReplayClientOptions) {
    this.autoAck = options.autoAck ?? true;
    this.random = options.random ?? Math.random;
    this.now = options.now ?? (() => Date.now());
    this.heartbeatMs = options.heartbeatMs ?? 15_000;
    this.gapTimeoutMs = options.gapTimeoutMs ?? 1_500;
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
    this.lostAt = null;
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

  /** Tell the server whether the customer is typing (kept in the session snapshot). */
  setTyping(isTyping: boolean): void {
    this.sendFrame({ type: "typing", is_typing: isTyping });
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
    this.stopHeartbeat();
    if (this.userClosed) return;
    if (this.lostAt === null) this.lostAt = this.now();
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
        this.startHeartbeat();
        this.update({
          status: "replaying",
          pod: frame.pod,
          reconnected: frame.reconnected,
          cursor: frame.cursor,
          cursorSource: frame.cursor_source,
          reconnectAttempt: 0,
          sessionVersion: frame.session_version ?? 0,
          handover: null,
          workflowState: frame.workflow_state ?? "",
          agent: frame.assigned_agent_name || frame.assigned_agent_id || "",
          typingStatus: frame.typing_status ?? "",
          error: frame.cursor_source === "redis" || frame.cursor_source === "none" ? "" : "Cursor store unavailable: replaying from the durable log.",
        });
        break;
      case "handover_started":
        this.update({
          status: "handover",
          sessionVersion: frame.session_version,
          handover: { phase: "started", oldPod: frame.old_pod, newPod: frame.new_pod, reason: frame.reason },
        });
        break;
      case "replay_started":
        this.update({
          status: this.state.status === "handover" ? "handover" : "replaying",
          lastReplay: { from: frame.from_sequence, to: frame.to_sequence, count: frame.count },
          replayedTotal: this.state.replayedTotal + frame.count,
        });
        break;
      case "replay_completed":
        this.finishRecovery();
        if (this.state.status !== "handover") this.update({ status: "connected" });
        // An ack that was in flight when the connection dropped is sent again.
        if (this.pendingAck && !this.autoAck) this.acknowledge(this.pendingAck);
        break;
      case "handover_completed":
        this.update({
          status: "connected",
          sessionVersion: frame.session_version,
          handover: {
            phase: "completed",
            oldPod: frame.old_pod,
            newPod: frame.new_pod,
            reason: this.state.handover?.reason ?? "",
            replayedFrom: frame.replayed_from_sequence,
            replayedTo: frame.replayed_to_sequence,
            replayedCount: frame.replayed_count,
            serverRecoveryMs: frame.recovery_ms,
          },
        });
        break;
      case "session_superseded":
        this.update({ error: "This conversation continued in another window or tab." });
        break;
      case "server_draining":
        this.update({ error: "The chat server is restarting. Moving you to another instance…" });
        break;
      case "pong":
        this.awaitingPong = false;
        break;
      case "resync_completed":
        break;
      case "chat_message":
        this.receive(frame as ReplayMessage);
        break;
      case "ack_confirmed":
        this.pendingAck = null;
        this.update({ cursor: frame.cursor, sessionVersion: frame.cursor?.session_version || this.state.sessionVersion });
        break;
      case "customer_message_accepted":
        break;
      case "error":
        this.onError(frame);
        break;
    }
  }

  private receive(message: ReplayMessage): void {
    this.awaitingPong = false; // any frame proves the connection is alive
    const previous = this.state.lastReceivedSequence;
    if (!message.replayed && previous > 0 && message.sequence_number > previous + 1) {
      this.update({ gapsDetected: this.state.gapsDetected + 1 });
      this.scheduleResync(previous);
    }
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

  private scheduleResync(afterSequence: number): void {
    if (this.gapTimer !== null) return;
    this.gapTimer = setTimeout(() => {
      this.gapTimer = null;
      const missing = this.state.messages.some((m) => m.sequence_number === afterSequence + 1);
      if (!missing) this.sendFrame({ type: "resync", after_sequence: afterSequence });
    }, this.gapTimeoutMs);
  }

  private finishRecovery(): void {
    if (this.lostAt !== null) {
      this.update({ recoveryMs: this.now() - this.lostAt });
      this.lostAt = null;
    }
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.awaitingPong = false;
    this.heartbeatTimer = setInterval(() => {
      if (this.awaitingPong) {
        // No reply since the last ping: treat it as a half-open connection, such as a partition.
        const socket = this.socket;
        this.onClose(1006, "heartbeat timed out");
        socket?.close(4000, "heartbeat timeout");
        return;
      }
      this.awaitingPong = true;
      this.sendFrame({ type: "ping" });
    }, this.heartbeatMs);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer !== null) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
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
    if (this.gapTimer !== null) clearTimeout(this.gapTimer);
    this.reconnectTimer = null;
    this.ackRetryTimer = null;
    this.gapTimer = null;
    this.stopHeartbeat();
  }

  private update(patch: Partial<ReplayState>): void {
    this.state = { ...this.state, ...patch };
    this.options.onState(this.state);
  }
}
