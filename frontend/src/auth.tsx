import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from "react";

const API = (import.meta.env.VITE_API_BASE as string) || "http://localhost:8000";
const KEY = "kcl-auth";

export type AuthMode = "readonly" | "full";

interface AuthState {
  token: string | null;
  mode: AuthMode;
  login: (username: string, password: string, mode: AuthMode) => Promise<void>;
  logout: () => void;
}

const Ctx = createContext<AuthState>({ token: null, mode: "readonly", login: async () => undefined, logout: () => undefined });

function load(): { token: string | null; mode: AuthMode } {
  try {
    const raw = sessionStorage.getItem(KEY);
    if (raw) {
      const p = JSON.parse(raw) as { token: string; mode: AuthMode };
      if (p.token) return p;
    }
  } catch { /* ignore */ }
  return { token: null, mode: "readonly" };
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
    const d = await r.json() as { token: string; mode: AuthMode };
    const next = { token: d.token, mode: d.mode };
    try { sessionStorage.setItem(KEY, JSON.stringify(next)); } catch { /* ignore */ }
    setAuth(next);
  }, []);

  const logout = useCallback(() => {
    try { sessionStorage.removeItem(KEY); } catch { /* ignore */ }
    setAuth({ token: null, mode: "readonly" });
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

  return <Ctx.Provider value={{ ...auth, login, logout }}>{children}</Ctx.Provider>;
}

/** 写操作报错转中文：401=登录失效，403=只读模式 */
export function friendlyWriteError(e: unknown): string {
  const msg = e instanceof Error ? e.message : String(e);
  if (/^401\b/.test(msg)) return "登录已失效（后端重启或 token 过期），请退出后重新登录";
  if (/^403\b/.test(msg)) return "只读模式：请退出后用【全部功能】模式重新登录";
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
  const [mode, setMode] = useState<AuthMode>("readonly");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const submit = async () => {
    if (!username.trim() || !password || busy) return;
    setBusy(true);
    setErr(null);
    try {
      await login(username.trim(), password, mode);
      window.location.hash = "#/dashboard";
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const features: [string, string, string][] = [
    ["◈", "集群概览", "实时掌握集群状态"],
    ["☰", "主题管理", "便捷的主题操作"],
    ["◫", "消费者组", "消费状态与积压分析"],
    ["⬢", "压测验证", "上线前大吞吐验证"],
  ];

  const modeCard = (m: AuthMode, title: string, desc: string) => (
    <button key={m} onClick={() => setMode(m)}
      className={`flex-1 rounded-xl border p-3 text-left transition-all ${mode === m ? "border-[rgba(56,189,248,0.55)] bg-[rgba(56,189,248,0.08)]" : "border-[var(--border-soft)] bg-[var(--panel-3)] hover:border-slate-600"}`}>
      <div className="flex items-center gap-2">
        <span className={`inline-flex h-4 w-4 items-center justify-center rounded-full ring-1 ${mode === m ? "bg-sky-500/30 ring-sky-400" : "ring-slate-600"}`}>
          {mode === m && <span className="h-1.5 w-1.5 rounded-full bg-sky-300" />}
        </span>
        <span className={`text-[12px] font-bold ${mode === m ? "text-slate-100" : "text-slate-400"}`}>{title}</span>
      </div>
      <div className="mt-1 text-[10.5px] leading-relaxed text-slate-500">{desc}</div>
    </button>
  );

  return (
    <div className="relative flex min-h-screen overflow-hidden" style={{ background: "#070d18" }}>
      {/* 整页背景图：全屏铺底 + 压暗过渡，缺图时自动隐藏走底色 */}
      <img src={`${import.meta.env.BASE_URL}login-bg.png`} alt="" aria-hidden
        onError={(e) => { (e.target as HTMLImageElement).style.display = "none"; }}
        className="pointer-events-none absolute inset-0 h-full w-full object-cover" />
      <div className="pointer-events-none absolute inset-0"
        style={{ background: "linear-gradient(to right, rgba(7,13,24,0.55), rgba(7,13,24,0.72)), radial-gradient(1000px 500px at 20% 0%, rgba(56,189,248,0.10), transparent)" }} />
      {/* 整页底纹：网格 + 光晕 */}
      <div className="pointer-events-none absolute inset-0" aria-hidden>
        <div className="absolute inset-0 opacity-[0.10] [background-image:linear-gradient(rgba(148,163,184,0.4)_1px,transparent_1px),linear-gradient(90deg,rgba(148,163,184,0.4)_1px,transparent_1px)] [background-size:44px_44px] [mask-image:radial-gradient(ellipse_75%_65%_at_50%_40%,black,transparent)]" />
        <div className="absolute -right-32 top-[-120px] h-96 w-96 rounded-full bg-sky-500/10 blur-[110px]" />
        <div className="absolute bottom-[-140px] left-[30%] h-80 w-80 rounded-full bg-cyan-400/[0.07] blur-[100px]" />
      </div>
      {/* 左：品牌区（透明，背景图透过） */}
      <div className="relative hidden w-[52%] flex-col overflow-hidden px-12 pb-8 pt-10 lg:flex">
        <div className="flex items-center gap-3">
          <span className="flex h-10 w-10 items-center justify-center rounded-xl bg-gradient-to-br from-sky-400 to-cyan-600 text-[18px] font-black text-[#04121b]">K</span>
          <span className="text-[19px] font-bold text-slate-100">Kafka Lab</span>
          <span className="ml-2 border-l border-slate-700 pl-3 text-[12px] text-slate-400">可视化管理 · 监控 · 运维</span>
        </div>
        <div className="mt-14 text-[30px] font-bold leading-snug text-slate-100">
          让 Kafka<br />更简单
        </div>
        <div className="mt-3 max-w-[420px] text-[13px] leading-relaxed text-slate-400">
          发送消息、拉取消费、压测验证，一站式看清 Kafka Client 的完整协议链路。
        </div>
        <div className="mt-8 grid max-w-[480px] grid-cols-4 gap-3">
          {features.map(([icon, t, d]) => (
            <div key={t}>
              <span className="flex h-9 w-9 items-center justify-center rounded-lg bg-[rgba(56,189,248,0.12)] text-[15px] text-sky-300">{icon}</span>
              <div className="mt-2 text-[12px] font-semibold text-slate-200">{t}</div>
              <div className="mt-0.5 text-[10.5px] leading-relaxed text-slate-500">{d}</div>
            </div>
          ))}
        </div>
        {/* 装饰：同心圆环 */}
        <div className="pointer-events-none absolute -bottom-40 -left-40 h-[480px] w-[480px] rounded-full border border-[rgba(56,189,248,0.12)]" />
        <div className="pointer-events-none absolute -bottom-24 -left-24 h-[320px] w-[320px] rounded-full border border-[rgba(56,189,248,0.14)]" />
        <div className="pointer-events-none absolute -bottom-8 -left-8 h-[160px] w-[160px] rounded-full bg-[rgba(56,189,248,0.08)] blur-2xl" />
        <div className="mt-auto flex items-center gap-2 text-[11px] text-slate-600">
          <span className="inline-block h-px w-6 bg-slate-700" />
          稳定 · 高效 · 安全
        </div>
      </div>
      {/* 右：登录卡（半透明毛玻璃，背景图透过） */}
      <div className="relative flex flex-1 items-center justify-center px-6 py-10">
        <div className="w-full max-w-[400px] rounded-2xl border border-white/10 bg-[rgba(10,17,30,0.55)] p-7 shadow-[0_20px_60px_-20px_rgba(0,0,0,0.8)] backdrop-blur-xl">
          <div className="flex items-center gap-2.5">
            <span className="flex h-9 w-9 items-center justify-center rounded-[10px] bg-gradient-to-br from-sky-400 to-cyan-600 text-[16px] font-black text-[#04121b]">K</span>
            <span className="text-[17px] font-bold text-slate-100">Kafka Lab</span>
          </div>
          <div className="mt-1.5 text-[12px] text-slate-500">欢迎登录，开启你的 Kafka 之旅</div>
          <label className="lbl mt-5">用户名</label>
          <div className="relative">
            <span className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-[13px] text-slate-500">◔</span>
            <input className="field mono !pl-9" value={username} placeholder="请输入用户名"
              onChange={(e) => setUsername(e.target.value)}
              onKeyDown={(e) => { if (e.key === "Enter") void submit(); }} />
          </div>
          <label className="lbl mt-3">密码</label>
          <div className="relative">
            <span className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-[13px] text-slate-500">◇</span>
            <input type={showPw ? "text" : "password"} className="field mono !pl-9 !pr-9" value={password} placeholder="请输入密码"
              onChange={(e) => setPassword(e.target.value)}
              onKeyDown={(e) => { if (e.key === "Enter") void submit(); }} />
            <button onClick={() => setShowPw((v) => !v)} title={showPw ? "隐藏" : "显示"}
              className="absolute right-2.5 top-1/2 -translate-y-1/2 text-[13px] text-slate-500 hover:text-slate-300">
              {showPw ? "◉" : "◎"}
            </button>
          </div>
          <div className="mb-2 mt-4 text-[12px] font-semibold text-slate-300">登录模式</div>
          <div className="flex gap-2">
            {modeCard("readonly", "只读巡检", "零侵入，写入功能隐藏")}
            {modeCard("full", "全部功能", "解锁写入，请确认集群")}
          </div>
          {err && <div className="alert err mt-3">{err}</div>}
          <button onClick={submit} disabled={busy || !username.trim() || !password}
            className="btn btn-info mt-4 w-full !py-2.5">
            {busy ? "登录中…" : "→ 登录"}
          </button>
          <div className="mt-3 text-center text-[10.5px] text-slate-600">账号见后端 users.json，运行时修改即时生效</div>
        </div>
      </div>
    </div>
  );
}
