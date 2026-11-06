/**
 * The client half of the delivery guarantee.
 *
 * - Outgoing messages stay in a persisted outbox until the server confirms that
 *   Kafka has them. They are resent with the same client_message_id after any
 *   failure, and the server turns those retries into no-ops.
 * - Incoming messages are deduplicated by message ID and released in sequence
 *   order. A later message is held until the gap before it is filled.
 * - A lost connection reconnects with exponential backoff and jitter, reporting
 *   the last contiguous sequence number so that only missing messages come back.
 */

export interface ChatMessage {
  type: "message";
  message_id: string;
  sequence_number: number;
  conversation_id: string;
  event_type: string;
  text: string;
  sender: { type?: string; id?: string; name?: string };
  metadata: Record<string, unknown>;
  timestamp: string;
  correlation_id: string;
  replayed: boolean;
}

export interface OutboxItem {
  clientMessageId: string;
  text: string;
  failed?: string;
}

export type ConnectionStatus = "connecting" | "online" | "reconnecting" | "closed";

export interface ConnectionInfo {
  status: ConnectionStatus;
  pod?: string;
  migrated?: boolean;
  previousPod?: string;
  lastReplayCount?: number;
  lastReplaySource?: string;
  reconnectAttempt?: number;
  closeReason?: string;
}

export interface ChatListener {
  onMessages(messages: ChatMessage[]): void;
  onOutbox(items: OutboxItem[]): void;
  onConnection(info: ConnectionInfo): void;
  onState?(frame: Record<string, unknown>): void;
  onTyping?(frame: { participant: string; role: string; is_typing: boolean }): void;
}

export interface SocketLike {
  send(data: string): void;
  close(code?: number, reason?: string): void;
  onopen: ((event: unknown) => void) | null;
  onmessage: ((event: { data: unknown }) => void) | null;
  onclose: ((event: { code: number; reason?: string }) => void) | null;
  onerror: ((event: unknown) => void) | null;
}

export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

export interface ChatOptions {
  url: string;
  token: string;
  conversationId: string;
  listener: ChatListener;
  socketFactory?: (url: string) => SocketLike;
  storage?: StorageLike;
  random?: () => number;
  gapTimeoutMs?: number;
  heartbeatMs?: number;
}

/** These closes will not be fixed by trying again. */
const TERMINAL_CLOSE_CODES: Record<number, string> = {
  4401: "Your session has expired. Please sign in again.",
  4403: "You don't have access to this conversation.",
  4404: "This conversation could not be found.",
  4409: "This conversation was opened in another window.",
};

const BACKOFF_BASE_MS = 500;
const BACKOFF_CAP_MS = 30_000;

export function backoffDelay(attempt: number, random: () => number = Math.random): number {
  const ceiling = Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * 2 ** attempt);
  return ceiling / 2 + random() * (ceiling / 2);
}

function newClientId(): string {
  return crypto.randomUUID();
}

export class ChatConnection {
  private socket: SocketLike | null = null;
  private stopped = false;
  private attempt = 0;
  private contiguous = 0;
  private readonly delivered = new Map<number, ChatMessage>();
  private readonly seenIds = new Set<string>();
  private readonly held = new Map<number, ChatMessage>();
  private outbox: OutboxItem[];
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private gapTimer: ReturnType<typeof setTimeout> | null = null;
  private retryTimer: ReturnType<typeof setTimeout> | null = null;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private awaitingPong = false;
  private ready = false;
  private info: ConnectionInfo = { status: "connecting" };
  private readonly storage: StorageLike | null;
  private readonly storageKey: string;
  private readonly random: () => number;
  private readonly gapTimeoutMs: number;
  private readonly heartbeatMs: number;

  constructor(private readonly options: ChatOptions) {
    this.storage = options.storage ?? (typeof localStorage === "undefined" ? null : localStorage);
    this.storageKey = `engagehub.outbox.${options.conversationId}`;
    this.random = options.random ?? Math.random;
    this.gapTimeoutMs = options.gapTimeoutMs ?? 2000;
    this.heartbeatMs = options.heartbeatMs ?? 20_000;
    this.outbox = this.loadOutbox();
  }

  start(): void {
    this.options.listener.onOutbox([...this.outbox]);
    this.open();
  }

  stop(): void {
    this.stopped = true;
    this.clearTimers();
    this.socket?.close(1000, "client closing");
    this.socket = null;
    this.setInfo({ status: "closed" });
  }

  /** Queue a message. It is durable in the outbox before any network attempt. */
  send(text: string): void {
    const item: OutboxItem = { clientMessageId: newClientId(), text };
    this.outbox = [...this.outbox, item];
    this.saveOutbox();
    if (this.ready) this.transmit(item);
  }

  /** Discard a message the server permanently refused. */
  dismiss(clientMessageId: string): void {
    this.removeFromOutbox(clientMessageId);
  }

  setTyping(isTyping: boolean): void {
    if (this.ready) this.sendFrame({ type: "typing", is_typing: isTyping });
  }

  get lastContiguousSeq(): number {
    return this.contiguous;
  }

  private open(): void {
    if (this.stopped) return;
    const factory = this.options.socketFactory ?? ((url: string) => new WebSocket(url) as unknown as SocketLike);
    const socket = factory(this.options.url);
    this.socket = socket;
    socket.onopen = () => {
      if (socket !== this.socket) return;
      this.sendFrame({
        type: "hello",
        token: this.options.token,
        conversation_id: this.options.conversationId,
        last_seen_seq: this.contiguous,
      });
    };
    socket.onmessage = (event) => {
      if (socket === this.socket) this.onFrame(JSON.parse(String(event.data)));
    };
    socket.onclose = (event) => {
      if (socket === this.socket) this.onClosed(event.code);
    };
    socket.onerror = () => undefined; // onclose always follows.
  }

  private onClosed(code: number): void {
    this.ready = false;
    this.socket = null;
    this.stopHeartbeat();
    if (this.stopped) return;
    const terminal = TERMINAL_CLOSE_CODES[code];
    if (terminal) {
      this.stopped = true;
      this.setInfo({ status: "closed", closeReason: terminal });
      return;
    }
    const delay = backoffDelay(this.attempt, this.random);
    this.attempt += 1;
    this.setInfo({ status: "reconnecting", reconnectAttempt: this.attempt });
    this.reconnectTimer = setTimeout(() => this.open(), delay);
  }

  private onFrame(frame: Record<string, any>): void {
    switch (frame.type) {
      case "welcome":
        this.attempt = 0;
        this.startHeartbeat();
        this.setInfo({
          status: "online",
          pod: frame.pod,
          migrated: frame.migrated,
          previousPod: frame.previous_pod,
          reconnectAttempt: 0,
        });
        this.options.listener.onState?.(frame);
        break;
      case "message":
        this.ingest([frame as unknown as ChatMessage]);
        break;
      case "message_ack":
        this.removeFromOutbox(frame.client_message_id);
        this.ingest([frame.message as ChatMessage]);
        break;
      case "replay_complete":
        this.onReplayComplete(frame);
        break;
      case "error":
        this.onServerError(frame);
        break;
      case "typing":
        this.options.listener.onTyping?.(frame as never);
        break;
      case "state":
        this.options.listener.onState?.(frame);
        break;
      case "pong":
        this.awaitingPong = false;
        break;
    }
  }

  private onReplayComplete(frame: Record<string, any>): void {
    this.setInfo({ lastReplayCount: frame.count, lastReplaySource: frame.source });
    if (frame.complete) {
      // The server has finished every reservation up to here, so any remaining
      // gap below through_seq can never be filled. It is safe to move past it.
      this.contiguous = Math.max(this.contiguous, frame.through_seq);
      this.releaseHeld();
    } else {
      this.scheduleGapRepair();
    }
    this.acknowledge();
    if (!this.ready) {
      this.ready = true;
      this.flushOutbox();
    }
  }

  private onServerError(frame: Record<string, any>): void {
    const id = frame.client_message_id as string | undefined;
    if (!id) return;
    if (frame.retryable) {
      if (this.retryTimer === null) {
        const delay = backoffDelay(1, this.random);
        this.retryTimer = setTimeout(() => {
          this.retryTimer = null;
          this.flushOutbox();
        }, delay);
      }
      return;
    }
    this.outbox = this.outbox.map((item) =>
      item.clientMessageId === id ? { ...item, failed: String(frame.code) } : item,
    );
    this.saveOutbox();
  }

  private ingest(messages: ChatMessage[]): void {
    for (const message of messages) {
      if (this.seenIds.has(message.message_id)) continue;
      this.seenIds.add(message.message_id);
      const seq = message.sequence_number;
      if (seq > this.contiguous + 1) {
        this.held.set(seq, message);
      } else {
        this.delivered.set(seq, message);
        this.contiguous = Math.max(this.contiguous, seq);
      }
    }
    this.releaseHeld();
    this.acknowledge();
    this.emitMessages();
    if (this.held.size > 0) this.scheduleGapRepair();
    else this.clearGapTimer();
  }

  private releaseHeld(): void {
    for (const [seq, message] of [...this.held].sort((a, b) => a[0] - b[0])) {
      if (seq > this.contiguous + 1) break;
      this.held.delete(seq);
      this.delivered.set(seq, message);
      this.contiguous = Math.max(this.contiguous, seq);
    }
    this.emitMessages();
  }

  private scheduleGapRepair(): void {
    if (this.gapTimer !== null) return;
    this.gapTimer = setTimeout(() => {
      this.gapTimer = null;
      if (this.ready && this.held.size > 0) {
        this.sendFrame({ type: "resync", after_seq: this.contiguous });
      }
    }, this.gapTimeoutMs);
  }

  private acknowledge(): void {
    if (this.contiguous === 0 || !this.socket) return;
    const last = this.delivered.get(this.contiguous);
    this.sendFrame({ type: "ack", up_to_seq: this.contiguous, message_id: last?.message_id ?? "" });
  }

  private flushOutbox(): void {
    for (const item of this.outbox) {
      if (!item.failed) this.transmit(item);
    }
  }

  private transmit(item: OutboxItem): void {
    this.sendFrame({ type: "message", client_message_id: item.clientMessageId, text: item.text });
  }

  private removeFromOutbox(clientMessageId: string): void {
    this.outbox = this.outbox.filter((item) => item.clientMessageId !== clientMessageId);
    this.saveOutbox();
  }

  private loadOutbox(): OutboxItem[] {
    try {
      return JSON.parse(this.storage?.getItem(this.storageKey) ?? "[]");
    } catch {
      return [];
    }
  }

  private saveOutbox(): void {
    try {
      if (this.outbox.length) this.storage?.setItem(this.storageKey, JSON.stringify(this.outbox));
      else this.storage?.removeItem(this.storageKey);
    } catch {
      // Storage can be unavailable. The outbox still works in memory for this page.
    }
    this.options.listener.onOutbox([...this.outbox]);
  }

  private sendFrame(frame: object): void {
    try {
      this.socket?.send(JSON.stringify(frame));
    } catch {
      // The socket is closing. onclose schedules the reconnect and the outbox keeps the message.
    }
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.awaitingPong = false;
    this.heartbeatTimer = setInterval(() => {
      if (this.awaitingPong) {
        // Half-open connection, such as a partition with no FIN: force a reconnect.
        const socket = this.socket;
        this.onClosed(1006);
        socket?.close();
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

  private clearGapTimer(): void {
    if (this.gapTimer !== null) clearTimeout(this.gapTimer);
    this.gapTimer = null;
  }

  private clearTimers(): void {
    if (this.reconnectTimer !== null) clearTimeout(this.reconnectTimer);
    if (this.retryTimer !== null) clearTimeout(this.retryTimer);
    this.clearGapTimer();
    this.stopHeartbeat();
  }

  private emitMessages(): void {
    this.options.listener.onMessages([...this.delivered.values()].sort((a, b) => a.sequence_number - b.sequence_number));
  }

  private setInfo(update: Partial<ConnectionInfo>): void {
    this.info = { ...this.info, ...update };
    this.options.listener.onConnection(this.info);
  }
}
