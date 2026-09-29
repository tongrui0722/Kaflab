import { createContext, useCallback, useContext, useEffect, useRef, useState, type ReactNode } from "react";
import type { SecurityForm } from "./api/producer";

export interface ConnConfig extends SecurityForm {
  id: string;
  name: string;
  bootstrap: string;
}

export interface ClusterHealth {
  connected: boolean | null;
  brokerCount?: number;
  topicCount?: number;
  brokers?: { id: number; host: string; port: number; rack?: string | null }[];
  controllerId?: number | null;
  error?: string;
}

const API = (import.meta.env.VITE_API_BASE as string) || "http://localhost:8000";
const ACTIVE_KEY = "kcl-active-cluster";

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(`${API}${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
  });
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json() as Promise<T>;
}

interface ConnCtx {
  clusters: ConnConfig[];
  activeId: string | null;
  conn: ConnConfig;
  health: Record<string, ClusterHealth>;
  loading: boolean;
  select: (id: string) => void;
  refresh: () => Promise<void>;
  save: (c: Partial<ConnConfig> & { bootstrap: string }) => Promise<ConnConfig>;
  remove: (id: string) => Promise<void>;
  probe: (id: string) => Promise<ClusterHealth>;
}

const FALLBACK: ConnConfig = {
  id: "local-plaintext", name: "本地 PLAINTEXT",
  bootstrap: "192.168.127.129:19092",
  securityProtocol: "PLAINTEXT", saslMechanism: "PLAIN",
  saslUsername: "", saslPassword: "",
};

const Ctx = createContext<ConnCtx>({
  clusters: [], activeId: null, conn: FALLBACK, health: {}, loading: true,
  select: () => undefined, refresh: async () => undefined,
  save: async () => FALLBACK, remove: async () => undefined,
  probe: async () => ({ connected: null }),
});

export function ConnProvider({ children }: { children: ReactNode }) {
  const [clusters, setClusters] = useState<ConnConfig[]>([]);
  const [activeId, setActiveId] = useState<string | null>(() => {
    try { return localStorage.getItem(ACTIVE_KEY); } catch { return null; }
  });
  const [health, setHealth] = useState<Record<string, ClusterHealth>>({});
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const list = await api<ConnConfig[]>("/api/clusters");
      setClusters(list);
      setActiveId((cur) => {
        if (cur && list.some((c) => c.id === cur)) return cur;
        const first = list[0]?.id ?? null;
        try {
          if (first) localStorage.setItem(ACTIVE_KEY, first);
          else localStorage.removeItem(ACTIVE_KEY);
        } catch { /* ignore */ }
        return first;
      });
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { refresh(); }, [refresh]);

  const select = useCallback((id: string) => {
    setActiveId(id);
    try { localStorage.setItem(ACTIVE_KEY, id); } catch { /* ignore */ }
  }, []);

  const save = useCallback(async (c: Partial<ConnConfig> & { bootstrap: string }) => {
    const saved = await api<ConnConfig>("/api/clusters", {
      method: "POST", body: JSON.stringify(c),
    });
    setClusters((prev) => {
      const i = prev.findIndex((x) => x.id === saved.id);
      if (i >= 0) { const n = [...prev]; n[i] = saved; return n; }
      return [...prev, saved];
    });
    return saved;
  }, []);

  const remove = useCallback(async (id: string) => {
    await api(`/api/clusters/${id}`, { method: "DELETE" });
    setClusters((prev) => prev.filter((x) => x.id !== id));
    setHealth((prev) => { const n = { ...prev }; delete n[id]; return n; });
  }, []);

  const probe = useCallback(async (id: string) => {
    try {
      const d = await api<{
        connected: boolean;
        brokerCount: number;
        topicCount: number;
        brokers: NonNullable<ClusterHealth["brokers"]>;
        controllerId?: number | null;
      }>(
        `/api/clusters/${id}/test`, { method: "POST", body: "{}" });
      const h: ClusterHealth = {
        connected: true, brokerCount: d.brokerCount, topicCount: d.topicCount,
        brokers: d.brokers, controllerId: d.controllerId,
      };
      setHealth((prev) => ({ ...prev, [id]: h }));
      return h;
    } catch (e) {
      const h: ClusterHealth = { connected: false, error: e instanceof Error ? e.message : String(e) };
      setHealth((prev) => ({ ...prev, [id]: h }));
      return h;
    }
  }, []);

  const conn = clusters.find((c) => c.id === activeId) ?? clusters[0] ?? FALLBACK;

  // 全局心跳：当前集群挂载即探测一次，之后每 20s 复测，窗口聚焦时补测一次。
  // 各页（侧边栏 / Consumer / Producer）只读 health，不再各自为政。
  const probeRef = useRef(probe);
  useEffect(() => { probeRef.current = probe; });
  useEffect(() => {
    const id = conn.id;
    if (!id) return;
    probeRef.current(id);
    const t = setInterval(() => { probeRef.current(id); }, 20000);
    const onFocus = () => { probeRef.current(id); };
    window.addEventListener("focus", onFocus);
    return () => { clearInterval(t); window.removeEventListener("focus", onFocus); };
  }, [conn.id]);
  return (
    <Ctx.Provider value={{ clusters, activeId: conn.id, conn, health, loading, select, refresh, save, remove, probe }}>
      {children}
    </Ctx.Provider>
  );
}

export function useConn() {
  return useContext(Ctx);
}

/** 兼容旧 presets 引用（已迁移为后端纳管，保留空壳避免引用报错）。 */
export const PRESETS: ConnConfig[] = [];
