import { type FormEvent, useCallback, useEffect, useState } from "react";
import { api, type Session } from "../api";
import { useChat } from "../useChat";
import { ChatPanel } from "./ChatPanel";

interface Entry {
  conversation_id: string;
  customer_id: string;
  status: string;
  priority: number;
  reason: string;
  intent: string;
  language: string;
  sentiment: number;
  tier: string;
  summary: string;
  position?: number;
  estimated_wait_seconds?: number;
}

interface QueueView {
  waiting: Entry[];
  offers: Entry[];
  assigned: Entry[];
}

function EntryCard({ entry, children, onOpen, selected }: { entry: Entry; children?: React.ReactNode; onOpen?: () => void; selected?: boolean }) {
  const body = (
    <>
      <div>
        <strong>{entry.customer_id}</strong>{" "}
        {entry.tier === "vip" && <span className="badge">VIP</span>}{" "}
        <span className="muted small">priority {entry.priority} · {entry.language}</span>
      </div>
      <div className="small muted">{entry.reason.replaceAll("_", " ")} · {entry.intent}</div>
      {entry.estimated_wait_seconds !== undefined && (
        <div className="small muted">Position {entry.position} · about {Math.max(1, Math.round(entry.estimated_wait_seconds / 60))} min</div>
      )}
    </>
  );
  return (
    <li className={`entry ${selected ? "selected" : ""}`}>
      {onOpen ? <button className="list-item" onClick={onOpen}>{body}</button> : body}
      {children && <div className="row">{children}</div>}
    </li>
  );
}

function ContextPanel({ token, conversationId, refresh }: { token: string; conversationId: string; refresh: number }) {
  const [context, setContext] = useState<any>(null);
  const [notes, setNotes] = useState<any[]>([]);
  const [draft, setDraft] = useState("");
  const [error, setError] = useState("");

  const load = useCallback(() => {
    api(`human/conversations/${conversationId}/context`, { token }).then(setContext).catch((e) => setError(e.message));
    api(`human/conversations/${conversationId}/notes`, { token }).then(setNotes).catch(() => undefined);
  }, [token, conversationId]);
  useEffect(() => { setContext(null); load(); }, [load, refresh]);

  async function addNote(event: FormEvent) {
    event.preventDefault();
    if (!draft.trim()) return;
    try {
      await api(`human/conversations/${conversationId}/notes`, { token, body: { text: draft.trim() } });
      setDraft("");
      load();
    } catch (e) {
      setError((e as Error).message);
    }
  }

  return (
    <aside className="stack">
      {error && <div className="banner banner-bad">{error}</div>}
      <section className="card stack">
        <h2>AI summary</h2>
        <p>{context?.summary || <span className="muted">No summary available.</span>}</p>
      </section>
      <section className="card stack">
        <h2>Customer</h2>
        {context?.customer ? (
          <p>{context.customer.name} {context.customer.tier === "vip" && <span className="badge">VIP</span>}
            <span className="muted small"> · language: {context.customer.language}</span></p>
        ) : <p className="muted">Customer details are unavailable.</p>}
      </section>
      <section className="card stack">
        <h2>Orders</h2>
        {context?.orders?.length === 0 && <p className="muted">No orders.</p>}
        <ul className="plain">
          {context?.orders?.map((o: any) => (
            <li key={o.id}><strong>{o.id}</strong> <span className="muted small">{o.status.replaceAll("_", " ")} · ${o.total.toFixed(2)} · {o.payment_status}</span></li>
          ))}
        </ul>
      </section>
      <section className="card stack">
        <h2>Internal notes</h2>
        <ul className="plain">
          {notes.map((n) => <li key={n.id}><span className="small muted">{n.author}:</span> {n.text}</li>)}
        </ul>
        <form className="row" onSubmit={addNote}>
          <input aria-label="Add internal note" placeholder="Not visible to the customer" value={draft} onChange={(e) => setDraft(e.target.value)} />
          <button disabled={!draft.trim()}>Add</button>
        </form>
      </section>
    </aside>
  );
}

export function AgentView({ session }: { session: Session }) {
  const { token, user } = session;
  const [status, setStatus] = useState("offline");
  const [queue, setQueue] = useState<QueueView>({ waiting: [], offers: [], assigned: [] });
  const [agents, setAgents] = useState<any[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [error, setError] = useState("");
  const [contextRefresh, setContextRefresh] = useState(0);
  const chat = useChat(token, activeId);

  const load = useCallback(() => {
    api<QueueView>("human/queue", { token }).then(setQueue).catch((e) => setError(e.message));
    api("human/agents", { token }).then(setAgents).catch(() => undefined);
  }, [token]);

  useEffect(() => {
    api("human/agents/me", { token }).then((me) => setStatus(me.status)).catch((e) => setError(e.message));
    load();
    const timer = setInterval(load, 2500);
    return () => clearInterval(timer);
  }, [token, load]);

  async function act(action: () => Promise<unknown>) {
    setError("");
    try {
      await action();
    } catch (e) {
      setError((e as Error).message);
    }
    load();
  }

  const changeStatus = (next: string) => act(async () => {
    const result = await api("human/agents/me/status", { token, method: "PUT", body: { status: next } });
    setStatus(result.status);
  });
  const accept = (id: string) => act(async () => {
    await api(`human/queue/${id}/accept`, { token, method: "POST" });
    setActiveId(id);
    setContextRefresh((n) => n + 1);
  });
  const reject = (id: string) => act(() => api(`human/queue/${id}/reject`, { token, method: "POST" }));
  const resolve = () => act(async () => {
    await api(`human/conversations/${activeId}/resolve`, { token, method: "POST" });
    setActiveId(null);
  });
  const escalate = () => act(() => api(`human/conversations/${activeId}/escalate`, { token, body: { reason: "supervisor review requested" } }));
  const transfer = (target: string) => act(async () => {
    await api(`human/conversations/${activeId}/transfer`, { token, body: { target_agent_id: target === "virtual" ? null : target } });
    setActiveId(null);
  });

  const colleagues = agents.filter((a) => a.id !== user.id);
  return (
    <div className="columns agent">
      <aside className="stack">
        <section className="card stack">
          <label>
            Your status
            <select value={status} onChange={(e) => changeStatus(e.target.value)}>
              <option value="available">Available</option>
              <option value="busy">Busy</option>
              <option value="offline">Offline</option>
            </select>
          </label>
        </section>
        {error && <div className="banner banner-bad" role="alert">{error}</div>}
        <section className="card stack">
          <h2>Offered to you {queue.offers.length > 0 && <span className="badge">{queue.offers.length}</span>}</h2>
          {queue.offers.length === 0 && <p className="muted">{status === "available" ? "No offers right now." : "Set your status to Available to receive offers."}</p>}
          <ul className="plain">
            {queue.offers.map((e) => (
              <EntryCard key={e.conversation_id} entry={e}>
                <button className="primary" onClick={() => accept(e.conversation_id)}>Accept</button>
                <button onClick={() => reject(e.conversation_id)}>Reject</button>
              </EntryCard>
            ))}
          </ul>
        </section>
        <section className="card stack">
          <h2>Your conversations</h2>
          {queue.assigned.length === 0 && <p className="muted">None assigned.</p>}
          <ul className="plain">
            {queue.assigned.map((e) => (
              <EntryCard key={e.conversation_id} entry={e} selected={e.conversation_id === activeId} onOpen={() => setActiveId(e.conversation_id)} />
            ))}
          </ul>
        </section>
        <section className="card stack">
          <h2>Waiting {queue.waiting.length > 0 && <span className="badge">{queue.waiting.length}</span>}</h2>
          {queue.waiting.length === 0 && <p className="muted">The queue is empty.</p>}
          <ul className="plain">
            {queue.waiting.map((e) => (
              <EntryCard key={e.conversation_id} entry={e}>
                <button onClick={() => accept(e.conversation_id)}>Take</button>
              </EntryCard>
            ))}
          </ul>
        </section>
      </aside>
      <main className="card chat-card">
        {activeId ? (
          <>
            <header className="chat-header wrap">
              <strong>Conversation</strong>
              <div className="row right">
                <select aria-label="Transfer conversation" value="" onChange={(e) => e.target.value && transfer(e.target.value)}>
                  <option value="">Transfer to…</option>
                  {colleagues.map((a) => <option key={a.id} value={a.id}>{a.name} ({a.status})</option>)}
                  <option value="virtual">Virtual assistant</option>
                </select>
                <button onClick={escalate}>Escalate</button>
                <button className="primary" onClick={resolve}>Resolve</button>
              </div>
            </header>
            <ChatPanel {...chat} selfSenderType="human_agent" placeholder="Reply to the customer" />
          </>
        ) : (
          <div className="empty">
            <h2>No conversation open</h2>
            <p className="muted">Accept an offer or open an assigned conversation.</p>
          </div>
        )}
      </main>
      {activeId
        ? <ContextPanel token={token} conversationId={activeId} refresh={contextRefresh} />
        : <aside className="stack"><section className="card stack"><h2>Team</h2><ul className="plain">
            {agents.map((a) => <li key={a.id}>{a.name} <span className="muted small">{a.status} · {a.active_conversations} active</span></li>)}
          </ul></section></aside>}
    </div>
  );
}
