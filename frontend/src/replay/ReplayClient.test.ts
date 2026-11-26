import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { backoffDelay, ReplayClient, type ReplayMessage, type ReplayState, type SocketLike } from "./ReplayClient";

class FakeSocket implements SocketLike {
  sent: any[] = [];
  closedWith: number | undefined;
  onopen: SocketLike["onopen"] = null;
  onmessage: SocketLike["onmessage"] = null;
  onclose: SocketLike["onclose"] = null;
  onerror: SocketLike["onerror"] = null;
  send(data: string) {
    this.sent.push(JSON.parse(data));
  }
  close(code?: number) {
    this.closedWith = code;
  }
  receive(frame: object) {
    this.onmessage?.({ data: JSON.stringify(frame) });
  }
  ofType(type: string) {
    return this.sent.filter((f) => f.type === type);
  }
}

function msg(seq: number, extra: Partial<ReplayMessage> = {}): ReplayMessage {
  return {
    type: "chat_message",
    conversation_id: "conversation-123",
    message_id: `message-${seq}`,
    sequence_number: seq,
    sender_type: "virtual_agent",
    sender_id: "order-tracking-agent",
    sender_name: "Order Tracking Agent",
    content: `reply ${seq}`,
    timestamp: "2026-11-12T12:00:00Z",
    correlation_id: "c",
    replayed: false,
    ...extra,
  };
}

function setup(autoAck = true) {
  const sockets: FakeSocket[] = [];
  let state = {} as ReplayState;
  const client = new ReplayClient({
    url: "ws://test/ws/customer/conversation-123",
    token: "tok",
    conversationId: "conversation-123",
    autoAck,
    random: () => 0.5,
    onState: (s) => (state = s),
    socketFactory: () => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    },
  });
  const current = () => sockets[sockets.length - 1];
  const handshake = (opts: { cursor?: number; replay?: ReplayMessage[]; reconnected?: boolean; source?: string } = {}) => {
    current().onopen?.({});
    const cursor = opts.cursor ?? 0;
    current().receive({
      type: "connection_ack", pod: "pod-a", reconnected: opts.reconnected ?? false, cursor_source: opts.source ?? "redis",
      cursor: { last_acknowledged_sequence: cursor, last_message_id: "", updated_at: "" }, latest_sequence: 0,
    });
    const replay = opts.replay ?? [];
    current().receive({ type: "replay_started", from_sequence: cursor + 1, to_sequence: replay.at(-1)?.sequence_number ?? cursor, count: replay.length });
    replay.forEach((m) => current().receive({ ...m, replayed: true }));
    current().receive({ type: "replay_completed", last_sequence: replay.at(-1)?.sequence_number ?? cursor });
  };
  client.connect();
  return { client, sockets, current, handshake, state: () => state };
}

const seqs = (state: ReplayState) => state.messages.map((m) => m.sequence_number);

beforeEach(() => {
  vi.useFakeTimers();
  vi.stubGlobal("crypto", { randomUUID: () => "client-id-0001" });
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("connection", () => {
  it("sends hello with the token and the last received sequence", () => {
    const { current } = setup();
    current().onopen?.({});
    expect(current().ofType("hello")[0]).toEqual({ type: "hello", token: "tok", last_received_sequence: 0 });
  });

  it("goes connecting -> replaying -> connected and records the replay range", () => {
    const { current, handshake, state } = setup();
    expect(state().status).toBe("connecting");
    current().onopen?.({});
    current().receive({ type: "connection_ack", pod: "pod-a", reconnected: false, cursor_source: "none", cursor: null });
    expect(state().status).toBe("replaying");
    current().receive({ type: "replay_started", from_sequence: 1, to_sequence: 0, count: 0 });
    current().receive({ type: "replay_completed", last_sequence: 0 });
    expect(state().status).toBe("connected");
    expect(state().lastReplay).toEqual({ from: 1, to: 0, count: 0 });
    void handshake;
  });
});

describe("messages", () => {
  it("shows each message once, in sequence order, even when replayed again", () => {
    const { current, handshake, state } = setup();
    handshake();
    current().receive(msg(2));
    current().receive(msg(1));
    current().receive(msg(2));
    current().receive({ ...msg(1), replayed: true });
    expect(seqs(state())).toEqual([1, 2]);
    expect(state().duplicatesIgnored).toBe(2);
    expect(state().lastReceivedSequence).toBe(2);
  });

  it("acknowledges every received message in auto mode", () => {
    const { current, handshake } = setup();
    handshake();
    current().receive(msg(1));
    current().receive(msg(2));
    expect(current().ofType("ack")).toEqual([
      { type: "ack", sequence_number: 1, message_id: "message-1" },
      { type: "ack", sequence_number: 2, message_id: "message-2" },
    ]);
  });

  it("acknowledges only on request in manual mode", () => {
    const { client, current, handshake, state } = setup(false);
    handshake();
    current().receive(msg(1));
    current().receive(msg(2));
    expect(current().ofType("ack")).toEqual([]);
    client.acknowledge(state().messages[0]);
    expect(current().ofType("ack")).toEqual([{ type: "ack", sequence_number: 1, message_id: "message-1" }]);
    current().receive({ type: "ack_confirmed", cursor: { last_acknowledged_sequence: 1, last_message_id: "message-1", updated_at: "" } });
    expect(state().cursor?.last_acknowledged_sequence).toBe(1);
  });

  it("retries an ack the server could not store", () => {
    const { current, handshake } = setup();
    handshake();
    current().receive(msg(1));
    current().receive({ type: "error", code: "cursor_unavailable", retryable: true, detail: "resend it" });
    vi.advanceTimersByTime(1100);
    expect(current().ofType("ack")).toHaveLength(2);
  });

  it("sends customer messages with an idempotency key", () => {
    const { client, current, handshake } = setup();
    handshake();
    client.sendCustomerMessage("thanks");
    expect(current().ofType("customer_message")).toEqual([
      { type: "customer_message", client_message_id: "client-id-0001", content: "thanks" },
    ]);
  });
});

describe("reconnecting", () => {
  it("reconnects automatically after an unexpected close and keeps the conversation", () => {
    const { current, handshake, sockets, state } = setup();
    handshake();
    current().receive(msg(1));
    current().onclose?.({ code: 1006 });
    expect(state().status).toBe("reconnecting");
    expect(state().error).toMatch(/unavailable/);
    vi.advanceTimersByTime(400);
    expect(sockets).toHaveLength(2);
    current().onopen?.({});
    expect(current().ofType("hello")[0].last_received_sequence).toBe(1);
  });

  it("merges replayed messages after a reconnect without duplicates", () => {
    const { current, handshake, state } = setup();
    handshake();
    [1, 2, 3, 4, 5].forEach((n) => current().receive(msg(n)));
    current().onclose?.({ code: 1012 });
    expect(state().error).toMatch(/restarted/);
    vi.advanceTimersByTime(400);
    handshake({ cursor: 3, reconnected: true, replay: [4, 5, 6, 7, 8].map((n) => msg(n)) });
    expect(seqs(state())).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    expect(state().reconnections).toBe(1);
    expect(state().reconnected).toBe(true);
    expect(state().status).toBe("connected");
  });

  it("backs off exponentially while the server stays down", () => {
    const { current, sockets } = setup();
    current().onclose?.({ code: 1006 });
    vi.advanceTimersByTime(374);
    expect(sockets).toHaveLength(1);
    vi.advanceTimersByTime(2);
    expect(sockets).toHaveLength(2);
    current().onclose?.({ code: 1006 });
    vi.advanceTimersByTime(749);
    expect(sockets).toHaveLength(2);
    vi.advanceTimersByTime(2);
    expect(sockets).toHaveLength(3);
    expect(backoffDelay(10, () => 1)).toBe(30_000);
  });

  it("does not reconnect after the user disconnects, and reconnects on request", () => {
    const { client, current, handshake, sockets, state } = setup();
    handshake();
    const socket = current();
    client.disconnect();
    expect(socket.closedWith).toBe(1000);
    expect(state().status).toBe("disconnected");
    socket.onclose?.({ code: 1000 });
    vi.advanceTimersByTime(60_000);
    expect(sockets).toHaveLength(1);
    client.connect();
    expect(sockets).toHaveLength(2);
    expect(state().status).toBe("reconnecting");
  });

  it.each([4401, 4404])("stops on terminal close %i", (code) => {
    const { current, sockets, state } = setup();
    current().onclose?.({ code });
    vi.advanceTimersByTime(60_000);
    expect(sockets).toHaveLength(1);
    expect(state().status).toBe("closed");
  });

  it("reports when the cursor came from a fallback", () => {
    const { handshake, state } = setup();
    handshake({ source: "client" });
    expect(state().cursorSource).toBe("client");
    expect(state().error).toMatch(/durable log/);
  });
});
