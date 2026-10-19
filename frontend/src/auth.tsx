import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from "react";

const API = (import.meta.env.VITE_API_BASE as string) || window.location.origin;
const KEY = "kcl-auth";

export type AuthMode = "readonly" | "full";

interface AuthState {
  token: string | null;
  mode: AuthMode;
  username: string | null;
  login: (username: string, password: string, mode: AuthMode) => Promise<void>;
  switchMode: (mode: AuthMode) => Promise<void>;
  logout: () => void;
}

const Ctx = createContext<AuthState>({ token: null, mode: "readonly", username: null, login: async () => undefined, switchMode: async () => undefined, logout: () => undefined });

function load(): Pick<AuthState, "token" | "mode" | "username"> {
  try {
    const raw = sessionStorage.getItem(KEY);
    if (raw) {
      const p = JSON.parse(raw) as { token?: string; mode?: AuthMode; username?: string };
      if (p.token) return { token: p.token, mode: p.mode === "full" ? "full" : "readonly", username: p.username || null };
    }
  } catch { /* ignore */ }
  return { token: null, mode: "readonly", username: null };
}

/** 非 hook 调用点（api 层）读取 token 组装请求头 */
export function authHeaders(): Record<string, string> {
  const { token } = load();
  return token ? { Authorization: `Bearer ${token}` } : {};
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [auth, setAuth] = useState(load);

  const login = useCallback(async (username: string, password: string, mode: AuthMode) => {
    const r = await fetch(`${API}/api/auth/login`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password, mode }),
    });
    if (!r.ok) throw new Error(r.status === 401 ? "用户名或密码错误" : await r.text());
    const d = await r.json() as { token: string; mode: AuthMode; username?: string };
    const next = { token: d.token, mode: d.mode, username: d.username || username.trim() };
    try { sessionStorage.setItem(KEY, JSON.stringify(next)); sessionStorage.removeItem("kcl-active-cluster"); } catch { /* ignore */ }
    setAuth(next);
  }, []);

  const switchMode = useCallback(async (mode: AuthMode) => {
    if (!auth.token) throw new Error("登录状态已失效，请重新登录");
    const response = await fetch(`${API}/api/auth/mode`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${auth.token}` },
      body: JSON.stringify({ mode }),
    });
    const body = await response.json().catch(() => ({})) as { mode?: AuthMode; detail?: string };
    if (!response.ok) {
      if (response.status === 404) {
        throw new Error("切换未成功，当前模式未改变。后端尚未加载模式切换接口，请重启 KAFLAB 后端后重试");
      }
      throw new Error(body.detail || `模式切换失败 (${response.status})`);
    }
    const next = { ...auth, mode: body.mode ?? mode };
    try { sessionStorage.setItem(KEY, JSON.stringify(next)); } catch { /* ignore */ }
    setAuth(next);
  }, [auth]);

  const logout = useCallback(() => {
    try { sessionStorage.removeItem(KEY); sessionStorage.removeItem("kcl-active-cluster"); } catch { /* ignore */ }
    setAuth({ token: null, mode: "readonly", username: null });
  }, []);

  // 启动时校验 token：后端重启会清空内存 token，失效直接踢回登录页，
  // 避免拿着旧 token 调写接口拿到误导性的 403
  useEffect(() => {
    if (!auth.token) return;
    fetch(`${API}/api/auth/me`, { headers: { Authorization: `Bearer ${auth.token}` } })
      .then((r) => { if (!r.ok) logout(); })
      .catch(() => undefined);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return <Ctx.Provider value={{ ...auth, login, switchMode, logout }}>{children}</Ctx.Provider>;
}

/** 写操作报错转中文：401=登录失效，403=只读模式 */
export function friendlyWriteError(e: unknown): string {
  const msg = e instanceof Error ? e.message : String(e);
  if (/^401\b/.test(msg)) return "登录已失效（后端重启或 token 过期），请退出后重新登录";
  if (/^403\b/.test(msg)) return "当前为只读模式，请在右上角切换到全部功能";
  return msg;
}

export function useAuth() {
  return useContext(Ctx);
}

export function LoginPage() {
  const { login } = useAuth();
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("");
  const [showPw, setShowPw] = useState(false);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const submit = async () => {
    if (!username.trim() || !password || busy) return;
    setBusy(true);
    setErr(null);
    try {
      await login(username.trim(), password, "readonly");
      window.location.hash = "#/clusters";
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="login-screen">
      <div className="login-bg-shade" aria-hidden="true" />
      <main className="login-stage">
        <div className="login-shell">
          <section className="login-card" aria-label="KAFLAB 登录">
            <div className="login-brand">
              <img className="login-logo" src="/images/kafka-symbol-blue.svg" alt="" />
              <h1><span className="login-brand-intro">欢迎使用</span><span className="login-wordmark">KAFLAB</span></h1>
            </div>
            <form onSubmit={(event) => { event.preventDefault(); void submit(); }}>
              <label className="login-label" htmlFor="login-username">用户名</label>
              <div className="login-input-wrap">
                <input id="login-username" autoComplete="username" className="field login-field" value={username} placeholder="请输入用户名"
                  onChange={(e) => setUsername(e.target.value)} />
              </div>
              <label className="login-label" htmlFor="login-password">密码</label>
              <div className="login-input-wrap">
                <input id="login-password" type={showPw ? "text" : "password"} autoComplete="current-password" className="field login-field login-field-password" value={password} placeholder="请输入密码"
                  onChange={(e) => setPassword(e.target.value)} />
                <button type="button" onClick={() => setShowPw((v) => !v)} title={showPw ? "隐藏密码" : "显示密码"} aria-label={showPw ? "隐藏密码" : "显示密码"}
                  aria-pressed={showPw} className="login-password-toggle">
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                    <path d="M2.5 12s3.5-5.5 9.5-5.5 9.5 5.5 9.5 5.5-3.5 5.5-9.5 5.5S2.5 12 2.5 12Z" />
                    <circle cx="12" cy="12" r="2.6" />
                    {!showPw && <path d="M3.5 3.5 20.5 20.5" />}
                  </svg>
                </button>
              </div>
              {err && <div className="alert err login-error" role="alert">{err}</div>}
              <button type="submit" disabled={busy || !username.trim() || !password}
                className="login-submit">
                {busy ? "正在登录…" : "登录"}
              </button>
            </form>
          </section>
        </div>
      </main>
    </div>
  );
}
