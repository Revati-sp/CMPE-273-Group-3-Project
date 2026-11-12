import { type FormEvent, useCallback, useEffect, useRef, useState } from "react";
import { api, type Session } from "../api";
import { ReplayClient, type ReplayState } from "../replay/ReplayClient";

const STATUS_TEXT: Record<string, string> = {
  idle: "Not connected",
  connecting: "Connecting…",
  replaying: "Replaying missed messages…",
  connected: "Connected",
  reconnecting: "Reconnecting…",
  disconnected: "Disconnected",
  closed: "Closed",
};

function readDevMode(): boolean {
  try {
    const stored = localStorage.getItem("engagehub.devMode");
    return stored === null ? true : stored === "1";
  } catch {
    return true;
  }
}

function socketUrlFor(conversationId: string): string {
  const scheme = location.protocol === "https:" ? "wss:" : "ws:";
  return `${scheme}//${location.host}/ws/customer/${encodeURIComponent(conversationId)}`;
}

export function ReplayDemoView({ session }: { session: Session }) {
  const { token } = session;
  const [conversationId, setConversationId] = useState("conversation-123");
  const [state, setState] = useState<ReplayState | null>(null);
  const [autoAck, setAutoAck] = useState(true);
  const [devMode, setDevMode] = useState(readDevMode);
  const [serverCursor, setServerCursor] = useState<any>(null);
  const [replyText, setReplyText] = useState("Your order is currently out for delivery.");
  const [replyCount, setReplyCount] = useState(1);
  const [draft, setDraft] = useState("");
  const [notice, setNotice] = useState("");
  const client = useRef<ReplayClient | null>(null);
  const activeId = useRef<string | null>(null);

  const refreshCursor = useCallback(async () => {
    if (!activeId.current) return;
    try {
      setServerCursor(await api(`chat/conversations/${encodeURIComponent(activeId.current)}/cursor`, { token }));
    } catch (e) {
      setServerCursor({ error: (e as Error).message });
    }
  }, [token]);

  useEffect(() => {
    const timer = setInterval(refreshCursor, 2000);
    return () => clearInterval(timer);
  }, [refreshCursor]);

  useEffect(() => () => client.current?.disconnect(), []);

  useEffect(() => {
    try {
      localStorage.setItem("engagehub.devMode", devMode ? "1" : "0");
    } catch {
      // Optional preference.
    }
  }, [devMode]);

  async function connect(event?: FormEvent) {
    event?.preventDefault();
    setNotice("");
    const id = conversationId.trim();
    if (activeId.current !== id || !client.current) {
      try {
        await api("chat/conversations", { token, body: { conversation_id: id } });
      } catch (e) {
        setNotice((e as Error).message);
        return;
      }
      client.current?.disconnect();
      activeId.current = id;
      client.current = new ReplayClient({ url: socketUrlFor(id), token, conversationId: id, autoAck, onState: setState });
    }
    client.current.connect();
    refreshCursor();
  }

  function disconnect() {
    client.current?.disconnect();
    refreshCursor();
  }

  async function publish(duplicateOfLast = false) {
    if (!activeId.current) return;
    setNotice("");
    const last = state?.messages.at(-1);
    const body = duplicateOfLast && last
      ? { count: 1, content: last.content, message_id: last.message_id }
      : { count: replyCount, content: replyCount > 1 ? "" : replyText };
    try {
      const result = await api(`chat/dev/fake-replies?conversation_id=${encodeURIComponent(activeId.current)}`, { token, body });
      setNotice(`Published ${result.message_ids.length} event(s) to ${result.topic}${duplicateOfLast ? " (duplicate message ID)" : ""}.`);
    } catch (e) {
      setNotice((e as Error).message);
    }
  }

  function sendCustomerMessage(event: FormEvent) {
    event.preventDefault();
    if (!draft.trim()) return;
    client.current?.sendCustomerMessage(draft.trim());
    setDraft("");
  }

  const status = state?.status ?? "idle";
  const connected = status === "connected" || status === "replaying";
  const statusClass = status === "connected" ? "banner-good" : status === "reconnecting" || status === "replaying" ? "banner-warn" : status === "closed" ? "banner-bad" : "";

  return (
    <div className="columns replay">
      <aside className="stack">
        <form className="card stack" onSubmit={connect}>
          <h2>Conversation</h2>
          <label>Conversation ID<input value={conversationId} onChange={(e) => setConversationId(e.target.value)} pattern="[A-Za-z0-9._:\-]{1,64}" required /></label>
          <div className="row">
            <button className="primary" type="submit" disabled={connected}>{state && status !== "idle" ? "Reconnect" : "Connect"}</button>
            <button type="button" onClick={disconnect} disabled={!client.current || status === "disconnected" || status === "idle"}>Disconnect</button>
          </div>
          <label className="inline">
            <input type="checkbox" checked={autoAck} onChange={(e) => { setAutoAck(e.target.checked); client.current?.setAutoAck(e.target.checked); }} />
            Acknowledge automatically
          </label>
          <label className="inline"><input type="checkbox" checked={devMode} onChange={(e) => setDevMode(e.target.checked)} /> Developer details</label>
        </form>
        <section className="card stack">
          <h2>Fake agent replies</h2>
          <p className="muted small">Published to Kafka topic <code>chat.replies</code>, the same path as <code>scripts/fake_reply_producer.py</code>.</p>
          <label>Reply text<input value={replyText} onChange={(e) => setReplyText(e.target.value)} disabled={replyCount > 1} /></label>
          <label>How many<input type="number" min={1} max={20} value={replyCount} onChange={(e) => setReplyCount(Math.max(1, Math.min(20, Number(e.target.value) || 1)))} /></label>
          <div className="row">
            <button onClick={() => publish(false)} disabled={!activeId.current}>Publish</button>
            <button onClick={() => publish(true)} disabled={!state?.messages.length} title="Republishes the newest message's ID">Publish duplicate</button>
          </div>
          {notice && <p className="small" role="status">{notice}</p>}
        </section>
      </aside>
      <main className="card chat-card">
        <header className="chat-header wrap">
          <strong>{activeId.current ?? "No conversation"}</strong>
          <span className={`pill ${statusClass}`} role="status">{STATUS_TEXT[status]}</span>
        </header>
        {state?.error && <div className={`banner ${status === "closed" ? "banner-bad" : "banner-warn"}`} role="alert">{state.error}</div>}
        {state?.lastReplay && state.reconnected && (
          <div className="banner banner-good" role="status">
            Reconnected{state.pod ? ` to ${state.pod.slice(0, 16)}` : ""}. Replayed {state.lastReplay.count} message(s)
            {state.lastReplay.count ? ` (#${state.lastReplay.from}–#${state.lastReplay.to})` : ""} from the durable log.
          </div>
        )}
        <ul className="chat-log" aria-live="polite">
          {!state?.messages.length && <li className="empty muted">{connected ? "No messages yet. Publish a fake reply." : "Connect to see messages."}</li>}
          {state?.messages.map((m) => {
            const mine = m.sender_type === "customer";
            const acked = (state.cursor?.last_acknowledged_sequence ?? 0) >= m.sequence_number;
            return (
              <li key={m.message_id} className={`bubble-row ${mine ? "mine" : "theirs"}`}>
                <div className={`bubble ${mine ? "bubble-mine" : "bubble-theirs"}`}>
                  <div className="bubble-sender">{m.sender_name || m.sender_id} <span className="sender-type">{m.sender_type.replace("_", " ")}</span></div>
                  <div className="bubble-text">{m.content}</div>
                  <div className="bubble-meta">
                    {new Date(m.timestamp).toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" })}
                    {devMode && <> · #{m.sequence_number}{m.replayed ? " · replayed" : ""}{acked ? " · acked" : ""}</>}
                    {!autoAck && !acked && connected && (
                      <> · <button className="link" onClick={() => client.current?.acknowledge(m)}>Ack up to here</button></>
                    )}
                  </div>
                </div>
              </li>
            );
          })}
        </ul>
        <form className="chat-input" onSubmit={sendCustomerMessage}>
          <input aria-label="Customer message" placeholder="Send a customer message" value={draft} onChange={(e) => setDraft(e.target.value)} disabled={!connected} maxLength={4000} />
          <button className="primary" disabled={!connected || !draft.trim()}>Send</button>
        </form>
      </main>
      <aside className="stack">
        <section className="card stack">
          <h2>Delivery state</h2>
          <dl className="facts">
            <dt>Last received sequence</dt><dd>{state?.lastReceivedSequence ?? 0}</dd>
            <dt>Cursor (this socket)</dt><dd>{state?.cursor?.last_acknowledged_sequence ?? 0}</dd>
            <dt>Messages shown</dt><dd>{state?.messages.length ?? 0}</dd>
            <dt>Duplicates ignored</dt><dd>{state?.duplicatesIgnored ?? 0}</dd>
            <dt>Reconnections</dt><dd>{state?.reconnections ?? 0}</dd>
            {devMode && <><dt>Server pod</dt><dd className="mono">{state?.pod || "–"}</dd><dt>Cursor source</dt><dd>{state?.cursorSource || "–"}</dd></>}
          </dl>
        </section>
        <section className="card stack">
          <h2>Redis cursor</h2>
          {serverCursor?.error ? (
            <div className="banner banner-bad">{serverCursor.error}</div>
          ) : serverCursor ? (
            <>
              <code className="small mono wrap-anywhere">{serverCursor.key}</code>
              <pre className="small mono">{JSON.stringify(serverCursor.cursor, null, 2) ?? "null"}</pre>
              <p className="small muted">Last delivered: {serverCursor.last_delivered_sequence}</p>
            </>
          ) : (
            <p className="muted small">Connect to a conversation.</p>
          )}
        </section>
      </aside>
    </div>
  );
}
