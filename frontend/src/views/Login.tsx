import { type FormEvent, useEffect, useState } from "react";
import { api, type Session, type Tenant } from "../api";

const DEMO_ACCOUNTS: Record<string, [string, string][]> = {
  acme: [["Customer", "dana@acme.example"], ["VIP customer", "vic@acme.example"], ["Agent", "alice@acme.example"], ["Agent", "bob@acme.example"], ["Admin", "admin@acme.example"]],
  globex: [["Customer", "sam@globex.example"], ["VIP customer", "val@globex.example"], ["Agent", "carol@globex.example"], ["Agent", "dave@globex.example"], ["Admin", "admin@globex.example"]],
};

export function Login({ onLogin }: { onLogin: (session: Session) => void }) {
  const [tenants, setTenants] = useState<Tenant[]>([]);
  const [tenantId, setTenantId] = useState("");
  const [mode, setMode] = useState<"login" | "register">("login");
  const [email, setEmail] = useState("");
  const [name, setName] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    api<Tenant[]>("tenant/tenants")
      .then((list) => {
        setTenants(list);
        setTenantId((current) => current || list[0]?.id || "");
      })
      .catch((e) => setError(e.message));
  }, []);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      const body = mode === "login" ? { tenant_id: tenantId, email, password } : { tenant_id: tenantId, email, password, name };
      const result = await api(`auth/${mode}`, { body });
      onLogin({ token: result.access_token, user: result.user });
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="login">
      <h1>EngageHub</h1>
      <p className="muted">Customer support that keeps going when servers don't.</p>
      <form className="card stack" onSubmit={submit}>
        <label>
          Business
          <select value={tenantId} onChange={(e) => setTenantId(e.target.value)} required>
            {tenants.map((t) => <option key={t.id} value={t.id}>{t.name}</option>)}
          </select>
        </label>
        {mode === "register" && (
          <label>Name<input value={name} onChange={(e) => setName(e.target.value)} required autoComplete="name" /></label>
        )}
        <label>Email<input type="email" value={email} onChange={(e) => setEmail(e.target.value)} required autoComplete="username" /></label>
        <label>
          Password
          <input type="password" value={password} minLength={mode === "register" ? 8 : undefined} onChange={(e) => setPassword(e.target.value)} required
            autoComplete={mode === "login" ? "current-password" : "new-password"} />
        </label>
        {error && <div className="banner banner-bad" role="alert">{error}</div>}
        <button className="primary" disabled={busy || !tenantId}>{mode === "login" ? "Sign in" : "Create account"}</button>
        <button type="button" className="link" onClick={() => { setMode(mode === "login" ? "register" : "login"); setError(""); }}>
          {mode === "login" ? "New customer? Create an account" : "Already registered? Sign in"}
        </button>
      </form>
      {DEMO_ACCOUNTS[tenantId] && (
        <section className="card demo">
          <h2>Demo accounts</h2>
          <p className="muted">Password: <code>engagehub-demo</code>. Select one to fill the form.</p>
          <ul>
            {DEMO_ACCOUNTS[tenantId].map(([role, address]) => (
              <li key={address}>
                <button type="button" className="link" onClick={() => { setMode("login"); setEmail(address); setPassword("engagehub-demo"); }}>
                  {address}
                </button>{" "}<span className="muted">{role}</span>
              </li>
            ))}
          </ul>
        </section>
      )}
    </main>
  );
}
