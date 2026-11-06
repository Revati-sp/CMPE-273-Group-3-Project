import { type FormEvent, useCallback, useEffect, useState } from "react";
import { api, type Session } from "../api";
import { useChat } from "../useChat";
import { ChatPanel } from "./ChatPanel";

interface Conversation {
  id: string;
  subject: string;
  status: string;
  rating: number | null;
  message_count: number;
  started_at: string;
  workflow_state: string;
  assigned_agent_name: string;
}

function ProductSearch({ token }: { token: string }) {
  const [query, setQuery] = useState("");
  const [products, setProducts] = useState<any[] | null>(null);
  const [error, setError] = useState("");

  async function search(event?: FormEvent) {
    event?.preventDefault();
    setError("");
    try {
      setProducts(query.trim()
        ? await api(`product/products?q=${encodeURIComponent(query.trim())}`, { token })
        : await api("product/products/recommendations?limit=4", { token }));
    } catch (e) {
      setError((e as Error).message);
    }
  }

  return (
    <section className="card stack">
      <h2>Products</h2>
      <form className="row" onSubmit={search}>
        <input aria-label="Search products" placeholder="Search" value={query} onChange={(e) => setQuery(e.target.value)} />
        <button>{query.trim() ? "Search" : "Top picks"}</button>
      </form>
      {error && <div className="banner banner-bad">{error}</div>}
      {products?.length === 0 && <p className="muted">No products match that search.</p>}
      <ul className="plain">
        {products?.map((p) => (
          <li key={p.id} className="product">
            <strong>{p.name}</strong> <span className="muted">${p.price.toFixed(2)}{p.stock === 0 ? " · out of stock" : ""}</span>
            <div className="muted small">{p.description}</div>
          </li>
        ))}
      </ul>
    </section>
  );
}

function Notifications({ token, refreshKey }: { token: string; refreshKey: number }) {
  const [items, setItems] = useState<any[]>([]);
  const load = useCallback(() => api("notification/notifications", { token }).then(setItems).catch(() => undefined), [token]);
  useEffect(() => {
    load();
    const timer = setInterval(load, 5000);
    return () => clearInterval(timer);
  }, [load, refreshKey]);

  async function markRead(id: string) {
    await api(`notification/notifications/${id}/read`, { token, method: "POST" }).catch(() => undefined);
    load();
  }

  const unread = items.filter((n) => !n.read).length;
  return (
    <section className="card stack">
      <h2>Notifications {unread > 0 && <span className="badge">{unread}</span>}</h2>
      {items.length === 0 && <p className="muted">Nothing yet.</p>}
      <ul className="plain">
        {items.slice(0, 5).map((n) => (
          <li key={n.id} className={n.read ? "muted" : ""}>
            <strong>{n.title}</strong>
            <div className="small">{n.body}</div>
            {!n.read && <button className="link small" onClick={() => markRead(n.id)}>Mark as read</button>}
          </li>
        ))}
      </ul>
    </section>
  );
}

function Rating({ token, conversation, onDone }: { token: string; conversation: Conversation; onDone: () => void }) {
  const [error, setError] = useState("");
  async function rate(rating: number) {
    try {
      await api(`session/conversations/${conversation.id}/rating`, { token, body: { rating } });
      onDone();
    } catch (e) {
      setError((e as Error).message);
    }
  }
  if (conversation.rating) return <p className="muted center">You rated this conversation {conversation.rating} out of 5. Thank you!</p>;
  return (
    <div className="rating">
      <span>How did we do?</span>
      {[1, 2, 3, 4, 5].map((n) => <button key={n} aria-label={`Rate ${n} out of 5`} onClick={() => rate(n)}>{n}</button>)}
      {error && <span className="banner banner-bad">{error}</span>}
    </div>
  );
}

export function CustomerView({ session }: { session: Session }) {
  const { token } = session;
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [workflow, setWorkflow] = useState("virtual");
  const [agentName, setAgentName] = useState("");
  const [error, setError] = useState("");
  const [refresh, setRefresh] = useState(0);

  const load = useCallback(() => api<Conversation[]>("session/conversations", { token }).then(setConversations).catch(() => undefined), [token]);
  useEffect(() => { load(); }, [load, refresh]);

  const chat = useChat(token, activeId, (frame) => {
    if (frame.type === "welcome") {
      setWorkflow(frame.workflow_state);
      setAgentName(frame.assigned_agent_name);
    } else if (frame.event_type === "agent_assigned") {
      setWorkflow("human_active");
      setAgentName(frame.payload.agent_name);
    } else if (frame.event_type === "handoff_requested" || frame.event_type === "agent_transferred") {
      setWorkflow("waiting_human");
      setAgentName("");
    } else if (frame.event_type === "agent_released") {
      setWorkflow("virtual");
      setAgentName("");
    } else if (frame.event_type === "conversation_resolved") {
      setWorkflow("resolved");
      setRefresh((n) => n + 1);
    }
  });

  async function start() {
    setError("");
    try {
      const created = await api("chat/conversations", { token, body: { subject: "" } });
      setActiveId(created.conversation_id);
      setWorkflow("virtual");
      setAgentName("");
      setTimeout(() => setRefresh((n) => n + 1), 800);
    } catch (e) {
      setError((e as Error).message);
    }
  }

  const active = conversations.find((c) => c.id === activeId);
  const resolved = workflow === "resolved";
  const statusText = resolved ? "Resolved" : workflow === "human_active" ? `Talking with ${agentName}` : workflow === "waiting_human" ? "Waiting for an agent" : "Virtual assistant";

  return (
    <div className="columns customer">
      <aside className="stack">
        <button className="primary" onClick={start}>New conversation</button>
        {error && <div className="banner banner-bad" role="alert">{error}</div>}
        <section className="card stack">
          <h2>Your conversations</h2>
          {conversations.length === 0 && <p className="muted">Start a conversation to see it here.</p>}
          <ul className="plain">
            {conversations.map((c) => (
              <li key={c.id}>
                <button className={`list-item ${c.id === activeId ? "selected" : ""}`} onClick={() => setActiveId(c.id)}>
                  {new Date(c.started_at).toLocaleString([], { dateStyle: "medium", timeStyle: "short" })}
                  <span className="muted small"> · {c.status} · {c.message_count} messages</span>
                </button>
              </li>
            ))}
          </ul>
        </section>
      </aside>
      <main className="card chat-card">
        {activeId ? (
          <>
            <header className="chat-header">
              <strong>Support</strong> <span className="muted">{statusText}</span>
              {!resolved && workflow === "virtual" && (
                <button className="right" onClick={() => chat.client.current?.send("I'd like to speak to a human agent")}>Talk to a person</button>
              )}
            </header>
            <ChatPanel {...chat} selfSenderType="customer" disabled={resolved} disabledText="This conversation is resolved."
              placeholder="Ask about products, orders, returns or billing" />
            {resolved && active && <Rating token={token} conversation={active} onDone={() => setRefresh((n) => n + 1)} />}
          </>
        ) : (
          <div className="empty">
            <h2>How can we help?</h2>
            <p className="muted">Start a new conversation, or open an earlier one.</p>
          </div>
        )}
      </main>
      <aside className="stack">
        <Notifications token={token} refreshKey={refresh} />
        <ProductSearch token={token} />
      </aside>
    </div>
  );
}
