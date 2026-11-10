import { useCallback, useEffect, useState } from "react";
import { api, type Session } from "../api";

function label(key: string): string {
  const text = key.replaceAll("_", " ");
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function Tile({ name, value, hint }: { name: string; value: string | number; hint?: string }) {
  return (
    <div className="card tile">
      <div className="tile-value">{value}</div>
      <div className="tile-name">{name}</div>
      {hint && <div className="muted small">{hint}</div>}
    </div>
  );
}

/** A one-series ranked bar chart. Direct labels name each bar, so no legend is needed. */
function BarList({ title, data, showTable }: { title: string; data: Record<string, number>; showTable: boolean }) {
  const entries = Object.entries(data);
  const max = Math.max(1, ...entries.map(([, v]) => v));
  return (
    <section className="card stack viz-root">
      <h2>{title}</h2>
      {entries.length === 0 && <p className="muted">No data yet.</p>}
      {showTable && entries.length > 0 ? (
        <table>
          <thead><tr><th>Category</th><th className="num">Count</th></tr></thead>
          <tbody>{entries.map(([k, v]) => <tr key={k}><td>{label(k)}</td><td className="num">{v}</td></tr>)}</tbody>
        </table>
      ) : (
        <ul className="plain bars">
          {entries.map(([key, value]) => (
            <li key={key} className="bar-row" title={`${label(key)}: ${value}`} tabIndex={0}>
              <span className="bar-label">{label(key)}</span>
              <span className="bar-track"><span className="bar" style={{ width: `${(value / max) * 100}%` }} /></span>
              <span className="bar-value">{value}</span>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

export function AdminView({ session }: { session: Session }) {
  const { token } = session;
  const [data, setData] = useState<any>(null);
  const [audit, setAudit] = useState<any[]>([]);
  const [workers, setWorkers] = useState<Record<string, { live_workers: number }>>({});
  const [days, setDays] = useState(30);
  const [showTable, setShowTable] = useState(false);
  const [error, setError] = useState("");

  const load = useCallback(() => {
    api(`analytics/analytics/overview?days=${days}`, { token }).then((d) => { setData(d); setError(""); }).catch((e) => setError(e.message));
    api("audit/audit?limit=25", { token }).then(setAudit).catch(() => undefined);
    api("orchestrator/agents/availability", { token }).then(setWorkers).catch(() => undefined);
  }, [token, days]);

  useEffect(() => {
    load();
    const timer = setInterval(load, 5000);
    return () => clearInterval(timer);
  }, [load]);

  return (
    <main className="admin stack">
      <div className="row">
        <label className="inline">Period
          <select value={days} onChange={(e) => setDays(Number(e.target.value))}>
            <option value={1}>Today</option>
            <option value={7}>7 days</option>
            <option value={30}>30 days</option>
          </select>
        </label>
        <label className="inline"><input type="checkbox" checked={showTable} onChange={(e) => setShowTable(e.target.checked)} /> Show charts as tables</label>
      </div>
      {error && <div className="banner banner-bad" role="alert">{error}</div>}
      {data && (
        <>
          <div className="tiles">
            <Tile name="Conversations" value={data.conversations_started} hint={`${data.conversations_resolved} resolved by agents`} />
            <Tile name="Messages" value={data.messages_total} />
            <Tile name="Escalation rate" value={`${Math.round(data.escalation_rate * 100)}%`} hint={`${data.escalations} handed to humans`} />
            <Tile name="Average human wait" value={`${data.average_human_wait_seconds}s`} />
            <Tile name="Average rating" value={data.average_rating ?? "–"} hint={`${data.ratings_count} rating(s)`} />
            <Tile name="Session migrations" value={data.session_migrations} hint={`${data.messages_replayed} messages replayed`} />
          </div>
          <div className="grid">
            <BarList title="Customer intents" data={data.intents} showTable={showTable} />
            <BarList title="Routed to virtual agent" data={data.routed_to_agent} showTable={showTable} />
            <BarList title="Escalation reasons" data={data.escalations_by_reason} showTable={showTable} />
            <BarList title="Human agent assignments" data={data.human_assignments} showTable={showTable} />
            <BarList title="Messages by type" data={data.messages_by_type} showTable={showTable} />
            <BarList title="Messages per day" data={data.messages_per_day} showTable={showTable} />
          </div>
        </>
      )}
      <section className="card stack">
        <h2>Virtual agent workers</h2>
        <ul className="plain workers">
          {Object.entries(workers).map(([agent, info]) => (
            <li key={agent}>
              <span className={info.live_workers ? "status-good" : "status-bad"}>{info.live_workers ? "● Running" : "▲ Down"}</span>{" "}
              {label(agent)} <span className="muted small">{info.live_workers} worker(s)</span>
            </li>
          ))}
        </ul>
      </section>
      <section className="card stack">
        <h2>Recent audit events</h2>
        <div className="table-wrap">
          <table>
            <thead><tr><th>Time</th><th>Action</th><th>Actor</th><th>Service</th></tr></thead>
            <tbody>
              {audit.map((r) => (
                <tr key={r.id} title={`Correlation ID ${r.correlation_id}`}>
                  <td>{new Date(r.occurred_at).toLocaleTimeString()}</td>
                  <td>{r.action}</td>
                  <td>{r.actor_id}</td>
                  <td>{r.producer_service}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}
