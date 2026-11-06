import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { backoffDelay, ChatConnection, type ChatMessage, type OutboxItem, type SocketLike, type StorageLike } from "./ChatConnection";

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

class MemoryStorage implements StorageLike {
  data = new Map<string, string>();
  getItem(key: string) {
    return this.data.get(key) ?? null;
  }
  setItem(key: string, value: string) {
    this.data.set(key, value);
  }
  removeItem(key: string) {
    this.data.delete(key);
  }
}

function msg(seq: number, id = `m${seq}`): ChatMessage {
  return {
    type: "message",
    message_id: id,
    sequence_number: seq,
    conversation_id: "c1",
    event_type: "agent_message",
    text: `text ${seq}`,
    sender: {},
    metadata: {},
    timestamp: "2026-01-01T00:00:00Z",
    correlation_id: "x",
    replayed: false,
  };
}

function setup(storage = new MemoryStorage()) {
  const sockets: FakeSocket[] = [];
  const state = { messages: [] as ChatMessage[], outbox: [] as OutboxItem[], status: "", info: {} as any };
  const connection = new ChatConnection({
    url: "ws://test",
    token: "tok",
    conversationId: "c1",
    storage,
    random: () => 0.5,
    socketFactory: () => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    },
    listener: {
      onMessages: (m) => (state.messages = m),
      onOutbox: (o) => (state.outbox = o),
      onConnection: (i) => {
        state.status = i.status;
        state.info = i;
      },
    },
  });
  connection.start();
  const current = () => sockets[sockets.length - 1];
  const handshake = (through = 0, complete = true) => {
    current().onopen?.({});
    current().receive({ type: "welcome", pod: "pod-a", migrated: false });
    current().receive({ type: "replay_complete", through_seq: through, count: 0, complete, source: "redis" });
  };
  return { connection, sockets, state, current, handshake, storage };
}

const seqs = (messages: ChatMessage[]) => messages.map((m) => m.sequence_number);

beforeEach(() => {
  vi.useFakeTimers();
  vi.stubGlobal("crypto", { randomUUID: (() => { let i = 0; return () => `client-id-${++i}`; })() });
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("backoff", () => {
  it("grows exponentially and is capped", () => {
    expect(backoffDelay(0, () => 1)).toBe(500);
    expect(backoffDelay(3, () => 1)).toBe(4000);
    expect(backoffDelay(30, () => 1)).toBe(30000);
    expect(backoffDelay(3, () => 0)).toBe(2000);
  });
});

describe("incoming messages", () => {
  it("drops duplicates by message ID", () => {
    const { current, handshake, state } = setup();
    handshake();
    current().receive(msg(1));
    current().receive(msg(1));
    current().receive({ ...msg(1), replayed: true });
    expect(seqs(state.messages)).toEqual([1]);
  });

  it("holds out-of-order messages until the gap fills", () => {
    const { current, handshake, state, connection } = setup();
    handshake();
    current().receive(msg(1));
    current().receive(msg(3));
    expect(seqs(state.messages)).toEqual([1]);
    expect(connection.lastContiguousSeq).toBe(1);
    current().receive(msg(2));
    expect(seqs(state.messages)).toEqual([1, 2, 3]);
    expect(connection.lastContiguousSeq).toBe(3);
  });

  it("requests a resync when a gap persists", () => {
    const { current, handshake } = setup();
    handshake();
    current().receive(msg(1));
    current().receive(msg(3));
    vi.advanceTimersByTime(2100);
    expect(current().ofType("resync")).toEqual([{ type: "resync", after_seq: 1 }]);
  });

  it("acknowledges only the contiguous position", () => {
    const { current, handshake } = setup();
    handshake();
    current().receive(msg(1));
    current().receive(msg(3));
    expect(current().ofType("ack").at(-1).up_to_seq).toBe(1);
  });

  it("moves past a permanent hole when the server reports a complete replay", () => {
    const { current, handshake, state, connection } = setup();
    handshake();
    current().receive(msg(1));
    current().receive(msg(3));
    current().receive({ type: "replay_complete", through_seq: 2, count: 0, complete: true, source: "durable" });
    expect(connection.lastContiguousSeq).toBe(3);
    expect(seqs(state.messages)).toEqual([1, 3]);
  });

  it("does not skip anything on a partial replay", () => {
    const { current, handshake, connection } = setup();
    handshake(0, false);
    expect(connection.lastContiguousSeq).toBe(0);
    vi.advanceTimersByTime(2100);
    current().receive(msg(5));
    expect(connection.lastContiguousSeq).toBe(0);
  });
});

describe("outgoing messages", () => {
  it("removes a message from the outbox only after the server acknowledges", () => {
    const { connection, current, handshake, state } = setup();
    handshake();
    connection.send("hello");
    expect(state.outbox).toHaveLength(1);
    const id = current().ofType("message")[0].client_message_id;
    current().receive({ type: "message_ack", client_message_id: id, duplicate: false, message: msg(1) });
    expect(state.outbox).toHaveLength(0);
    expect(seqs(state.messages)).toEqual([1]);
  });

  it("resends unacknowledged messages with the same ID after reconnecting", () => {
    const { connection, current, handshake, sockets } = setup();
    handshake();
    connection.send("hello");
    const id = current().ofType("message")[0].client_message_id;
    current().onclose?.({ code: 1006 });
    vi.advanceTimersByTime(1000);
    expect(sockets).toHaveLength(2);
    handshake();
    expect(current().ofType("message")).toEqual([{ type: "message", client_message_id: id, text: "hello" }]);
  });

  it("waits for replay to finish before resending", () => {
    const { connection, current, handshake } = setup();
    handshake();
    connection.send("hello");
    current().onclose?.({ code: 1012 });
    vi.advanceTimersByTime(1000);
    current().onopen?.({});
    current().receive({ type: "welcome", pod: "pod-b" });
    expect(current().ofType("message")).toEqual([]);
    current().receive({ type: "replay_complete", through_seq: 0, count: 0, complete: true, source: "redis" });
    expect(current().ofType("message")).toHaveLength(1);
  });

  it("queues messages sent while offline", () => {
    const { connection, current, handshake, state } = setup();
    handshake();
    current().onclose?.({ code: 1006 });
    connection.send("while offline");
    expect(state.outbox).toHaveLength(1);
    vi.advanceTimersByTime(1000);
    handshake();
    expect(current().ofType("message")[0].text).toBe("while offline");
  });

  it("recovers the outbox after a page reload", () => {
    const storage = new MemoryStorage();
    const first = setup(storage);
    first.handshake();
    first.connection.send("survives reload");
    const id = first.current().ofType("message")[0].client_message_id;

    const second = setup(storage);
    second.handshake();
    expect(second.current().ofType("message")).toEqual([{ type: "message", client_message_id: id, text: "survives reload" }]);
  });

  it("retries a retryable error and keeps a permanent one visible", () => {
    const { connection, current, handshake, state } = setup();
    handshake();
    connection.send("hello");
    const id = current().ofType("message")[0].client_message_id;
    current().receive({ type: "error", code: "temporarily_unavailable", retryable: true, client_message_id: id });
    vi.advanceTimersByTime(1100);
    expect(current().ofType("message")).toHaveLength(2);
    expect(current().ofType("message")[1].client_message_id).toBe(id);

    current().receive({ type: "error", code: "conversation_resolved", retryable: false, client_message_id: id });
    expect(state.outbox[0].failed).toBe("conversation_resolved");
    connection.dismiss(id);
    expect(state.outbox).toHaveLength(0);
  });
});

describe("reconnecting", () => {
  it("reports the last contiguous sequence number in hello", () => {
    const { current, handshake } = setup();
    handshake();
    current().receive(msg(1));
    current().receive(msg(2));
    current().onclose?.({ code: 1006 });
    vi.advanceTimersByTime(1000);
    current().onopen?.({});
    expect(current().ofType("hello")[0]).toMatchObject({ last_seen_seq: 2, conversation_id: "c1" });
  });

  it("backs off further with each failed attempt and resets after success", () => {
    const { current, sockets, handshake, state } = setup();
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
    expect(state.status).toBe("reconnecting");
    handshake();
    expect(state.status).toBe("online");
    current().onclose?.({ code: 1006 });
    vi.advanceTimersByTime(376);
    expect(sockets).toHaveLength(4);
  });

  it.each([4401, 4403, 4404, 4409])("does not reconnect after terminal close %i", (code) => {
    const { current, sockets, state } = setup();
    current().onclose?.({ code });
    vi.advanceTimersByTime(60_000);
    expect(sockets).toHaveLength(1);
    expect(state.status).toBe("closed");
    expect(state.info.closeReason).toBeTruthy();
  });

  it("detects a half-open connection through missed pongs", () => {
    const { current, handshake, sockets, state } = setup();
    handshake();
    vi.advanceTimersByTime(20_000);
    expect(current().ofType("ping")).toHaveLength(1);
    vi.advanceTimersByTime(20_000);
    expect(state.status).toBe("reconnecting");
    vi.advanceTimersByTime(1000);
    expect(sockets).toHaveLength(2);
  });

  it("ignores events from a socket it has replaced", () => {
    const { current, handshake, sockets, state } = setup();
    handshake();
    const old = current();
    old.onclose?.({ code: 1006 });
    vi.advanceTimersByTime(1000);
    handshake();
    old.receive(msg(9));
    old.onclose?.({ code: 1006 });
    expect(state.messages).toEqual([]);
    expect(state.status).toBe("online");
    expect(sockets).toHaveLength(2);
  });

  it("stop closes cleanly and does not reconnect", () => {
    const { connection, current, sockets } = setup();
    const socket = current();
    connection.stop();
    expect(socket.closedWith).toBe(1000);
    vi.advanceTimersByTime(60_000);
    expect(sockets).toHaveLength(1);
  });
});
