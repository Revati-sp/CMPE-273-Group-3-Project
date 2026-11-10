import { useEffect, useState } from "react";
import { api, type Session, type Tenant } from "./api";
import { AdminView } from "./views/AdminView";
import { AgentView } from "./views/AgentView";
import { CustomerView } from "./views/CustomerView";
import { Login } from "./views/Login";

const STORAGE_KEY = "engagehub.session";

function loadSession(): Session | null {
  try {
    return JSON.parse(sessionStorage.getItem(STORAGE_KEY) ?? "null");
  } catch {
    return null;
  }
}

const ROLE_LABEL = { customer: "Customer", agent: "Human agent", admin: "Administrator" };

export function App() {
  const [session, setSession] = useState<Session | null>(loadSession);
  const [tenant, setTenant] = useState<Tenant | null>(null);

  useEffect(() => {
    if (!session) return;
    api<Tenant>(`tenant/tenants/${session.user.tenant_id}`).then(setTenant).catch(() => undefined);
  }, [session]);

  function login(next: Session) {
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(next));
    } catch {
      // Storage is optional: the session then lasts until the page reloads.
    }
    setSession(next);
  }

  function logout() {
    try {
      sessionStorage.removeItem(STORAGE_KEY);
    } catch {
      // Nothing to clear.
    }
    setSession(null);
    setTenant(null);
  }

  if (!session) return <Login onLogin={login} />;
  const { user } = session;
  return (
    <>
      <header className="topbar">
        <strong>{tenant?.name ?? user.tenant_id}</strong>
        <span className="muted">{ROLE_LABEL[user.role]}</span>
        <span className="right">{user.name}</span>
        <button onClick={logout}>Sign out</button>
      </header>
      {user.role === "customer" && <CustomerView session={session} />}
      {user.role === "agent" && <AgentView session={session} />}
      {user.role === "admin" && <AdminView session={session} />}
    </>
  );
}
