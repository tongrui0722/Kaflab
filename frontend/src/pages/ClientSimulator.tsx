import { apiFetch, apiWebSocket } from "../api/http";
import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import { authHeaders, useAuth } from "../auth";
import { useConn } from "../conn";
import { PageHeader } from "../components/bits";
import { balanceLimits, editLimits, limitError, type LimitDrivers, type LimitField } from "../simLimits";
import { retainLogWindow } from "../simLogWindow";

const API = (import.meta.env.VITE_API_BASE as string) || window.location.origin;
type Kind = "producer" | "consumer";
type ClientId = "python" | "java" | "node" | "go";
interface Version { version: string; available: boolean }
interface Client { id: ClientId; label: string; versions: Version[] }
interface ConfigOption { key: string; value: number | string; type: "integer" | "choice"; min?: number; max?: number; choices?: string[]; label: string }
interface Run {
  id: string; kind: Kind; client: ClientId; version: string; status: string;
  createdAt: string; startedAt?: string | null; endedAt?: string | null; pid?: number | null;
  exitCode?: number | null; error?: string; sent: number; received: number; errors: number;
  logTruncated?: boolean; config: { bootstrap: string; topic: string; groupId?: string; logLevel?: string };
}
interface LogEntry { seq: number; time: string; channel: string; source?: "system" | "adapter" | "client"; message: string; raw?: string; rawTruncated?: boolean; event?: string; partition?: number; offset?: number }

async function simApi<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await apiFetch(`${API}/api/client-sim${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...authHeaders(), ...(init?.headers ?? {}) },
  });
  const data = await response.json().catch(() => ({})) as T & { detail?: string };
  if (!response.ok) {
    if (response.status === 404 && data.detail === "Not Found") {
      throw new Error(`当前后端未加载客户端模拟接口（${API}）。请确认后端从本项目启动，重启后重新登录并刷新页面。`);
    }
    throw new Error(data.detail || `请求失败 (${response.status})`);
  }
  return data;
}

const defaults = {
  client: "python" as ClientId, version: "2.16.0", bootstrap: "", topic: "",
  securityProtocol: "PLAINTEXT", saslMechanism: "PLAIN", saslUsername: "", saslPassword: "", caPath: "",
  key: "", value: "kaflab-{runId}-{seq}", count: "10", rate: "1", durationSeconds: "10",
  connectionTimeoutMs: "10000", requestTimeoutMs: "30000", maxMessageBytes: "1048576",
  retries: "3", maxFetchBytes: "10485760", fetchMaxWaitMs: "500",
  groupId: "", offsetStart: "latest", commit: false, partition: "", logLevel: "info",
};
const STATUS: Record<string, string> = {
  starting: "启动中", running: "运行中", stopping: "停止中", stopped: "已停止",
  completed: "已完成", failed: "失败", interrupted: "已中断",
};
const LANG: Record<ClientId, string> = { python: "Python", java: "Java", node: "Node.js", go: "Go" };
const logClock = new Intl.DateTimeFormat("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
const runClock = new Intl.DateTimeFormat("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false });
const localTime = (value: string, formatter: Intl.DateTimeFormat) => {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : formatter.format(date);
};

function isErrorLog(entry: LogEntry): boolean {
  if (entry.event === "error") return true;
  if (entry.source === "adapter" || entry.source === "system") return false;
  const line = (entry.raw ?? entry.message).trimStart();
  if (line.startsWith("{")) {
    try {
      const native = JSON.parse(line) as { level?: unknown };
      if (typeof native.level === "string") return /^(ERROR|FATAL)$/i.test(native.level);
    } catch { /* 普通文本日志继续按前缀识别。 */ }
  }
  return /^(ERROR|FATAL|Exception|Traceback)\b/i.test(line);
}

function configTemplate(options: ConfigOption[], client: ClientId, version: string, kind: Kind): string {
  return [`# ${LANG[client]} ${version} · ${kind === "producer" ? "生产端" : "消费端"}客户端参数`,
    "# 修改冒号后的值；以 # 开头的行不会参与运行", "",
    ...options.map((option) => `${option.key}: ${option.value}  # ${option.label}`)].join("\n");
}

function parseConfig(text: string, options: ConfigOption[], client: ClientId, kind: Kind): { values: Record<string, number | string>; error: string } {
  const known = new Map(options.map((option) => [option.key, option]));
  const values: Record<string, number | string> = {};
  for (const [index, raw] of text.split(/\r?\n/).entries()) {
    const line = raw.trim();
    if (!line || line.startsWith("#")) continue;
    const match = /^([\w.-]+)\s*:\s*(.*?)\s*(?:\s+#.*)?$/.exec(line);
    if (!match) return { values, error: `第 ${index + 1} 行格式应为 参数名: 值` };
    const [, key, value] = match;
    const option = known.get(key);
    if (!option) return { values, error: `第 ${index + 1} 行：当前版本不支持 ${key}` };
    if (Object.prototype.hasOwnProperty.call(values, key)) return { values, error: `第 ${index + 1} 行：${key} 重复` };
    if (option.type === "integer") {
      if (!/^-?\d+$/.test(value) || Number(value) < (option.min ?? 0) || Number(value) > (option.max ?? Infinity))
        return { values, error: `第 ${index + 1} 行：${key} 需为 ${option.min}–${option.max} 的整数` };
      values[key] = Number(value);
    } else {
      if (!option.choices?.includes(value)) return { values, error: `第 ${index + 1} 行：${key} 可选 ${option.choices?.join(" / ")}` };
      values[key] = value;
    }
  }
  const resolved = Object.fromEntries(options.map((option) => [option.key, values[option.key] ?? option.value]));
  if (client === "java") {
    if (Number(resolved["socket.connection.setup.timeout.max.ms"]) < Number(resolved["socket.connection.setup.timeout.ms"]))
      return { values, error: "连接超时上限不能小于连接超时" };
    if (kind === "producer" && Number(resolved["delivery.timeout.ms"]) < Number(resolved["request.timeout.ms"]) + Number(resolved["linger.ms"]))
      return { values, error: "delivery.timeout.ms 不能小于 request.timeout.ms + linger.ms" };
    if (kind === "producer" && Number(resolved["batch.size"]) > Number(resolved["buffer.memory"]))
      return { values, error: "batch.size 不能大于 buffer.memory" };
  }
  if (kind === "consumer" && (client === "java" || client === "node")) {
    const heartbeat = client === "java" ? "heartbeat.interval.ms" : "heartbeatInterval";
    const session = client === "java" ? "session.timeout.ms" : "sessionTimeout";
    if (Number(resolved[heartbeat]) >= Number(resolved[session])) return { values, error: "心跳间隔必须小于消费组会话超时" };
  }
  return { values, error: "" };
}

export function ClientSimulatorPage({ kind }: { kind: Kind }) {
  const { mode } = useAuth();
  const { activeId, clusters, conn, loading: clustersLoading } = useConn();
  const [form, setForm] = useState(defaults);
  const [managedClusterId, setManagedClusterId] = useState("");
  const [configOptions, setConfigOptions] = useState<ConfigOption[]>([]);
  const [configText, setConfigText] = useState("");
  const [configSchemaKey, setConfigSchemaKey] = useState("");
  const [configError, setConfigError] = useState("");
  const [configOpen, setConfigOpen] = useState(false);
  const configDrafts = useRef<Record<string, string>>({});
  const [autoBalance, setAutoBalance] = useState(true);
  const [limitDrivers, setLimitDrivers] = useState<LimitDrivers>(["durationSeconds", "count"]);
  const [catalog, setCatalog] = useState<Client[]>([]);
  const [catalogLoaded, setCatalogLoaded] = useState(false);
  const [runs, setRuns] = useState<Run[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [detail, setDetail] = useState<Run | null>(null);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [logFilter, setLogFilter] = useState("all");
  const [logView, setLogView] = useState<"events" | "raw">("events");
  const [topics, setTopics] = useState<string[]>([]);
  const [topicsLoaded, setTopicsLoaded] = useState(false);
  const [topicsLoading, setTopicsLoading] = useState(false);
  const [topicsError, setTopicsError] = useState("");
  const [topicOpen, setTopicOpen] = useState(false);
  const [topicCursor, setTopicCursor] = useState(0);
  const [topicPartitions, setTopicPartitions] = useState("1");
  const [topicReplicationFactor, setTopicReplicationFactor] = useState("1");
  const topicRequest = useRef(0);
  const cursor = useRef(0);
  const loadingDetail = useRef(false);
  const consoleRef = useRef<HTMLDivElement>(null);
  const configButtonRef = useRef<HTMLButtonElement>(null);
  const configDrawerRef = useRef<HTMLElement>(null);
  const configScrollRef = useRef<HTMLDivElement>(null);
  const topicFieldRef = useRef<HTMLDivElement>(null);
  const topicComboRef = useRef<HTMLDivElement>(null);
  const topicPopoverRef = useRef<HTMLDivElement>(null);
  const topicListRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!configOpen) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") setConfigOpen(false);
      if (event.key !== "Tab") return;
      const items = configDrawerRef.current?.querySelectorAll<HTMLElement>('button:not(:disabled), textarea:not(:disabled)');
      if (!items?.length) return;
      const first = items[0], last = items[items.length - 1];
      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
    };
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    document.addEventListener("keydown", onKeyDown);
    return () => { document.body.style.overflow = previousOverflow; document.removeEventListener("keydown", onKeyDown); configButtonRef.current?.focus(); };
  }, [configOpen]);
  const chosen = catalog.find((item) => item.id === form.client);
  const versionReady = chosen?.versions.some((item) => item.version === form.version && item.available) ?? false;
  const active = detail && ["starting", "running", "stopping"].includes(detail.status);
  const hasSelectedCluster = Boolean(activeId && clusters.some((cluster) => cluster.id === activeId));
  const limitValues = { count: form.count, durationSeconds: form.durationSeconds, rate: form.rate };
  const limitsProblem = limitError(limitValues, kind === "producer");
  const wantedConfigKey = `${kind}/${form.client}/${form.version}`;
  const parsedConfig = parseConfig(configText, configOptions, form.client, kind);
  const advancedProblem = configError || (configSchemaKey !== wantedConfigKey ? "正在读取客户端参数模板…" : parsedConfig.error);
  const derivedLimit = (["count", "durationSeconds", "rate"] as const).find((field) => !limitDrivers.includes(field));
  const topicName = form.topic.trim();
  const filteredTopics = topics.filter((name) => name.toLocaleLowerCase().includes(topicName.toLocaleLowerCase()));
  const topicExists = topics.includes(topicName);
  const newTopic = kind === "producer" && Boolean(topicName) && (!topicsLoaded || !topicExists);
  const topicConfigProblem = newTopic && !/^[a-zA-Z0-9._-]{1,249}$/.test(topicName)
    ? "Topic 名称只能包含字母、数字、点、下划线和连字符" : newTopic && (!Number.isInteger(Number(topicPartitions)) || Number(topicPartitions) < 1 || Number(topicPartitions) > 1000 ||
    !Number.isInteger(Number(topicReplicationFactor)) || Number(topicReplicationFactor) < 1 || Number(topicReplicationFactor) > 100)
    ? "分区数须为 1–1000，副本数须为 1–100" : "";

  useLayoutEffect(() => {
    if (!topicOpen) return;
    const scroller = configScrollRef.current;
    const field = topicFieldRef.current;
    const popover = topicPopoverRef.current;
    if (!scroller || !field || !popover) return;
    const top = scroller.getBoundingClientRect().top + 5;
    const fieldTop = field.getBoundingClientRect().top;
    if (fieldTop < top) scroller.scrollTop += fieldTop - top;
    const bottom = scroller.getBoundingClientRect().bottom - 5;
    const popoverBottom = popover.getBoundingClientRect().bottom;
    if (popoverBottom > bottom) {
      // 展开列表时把 Topic 标签与输入框一起放在可见区顶部，避免上一字段只露出半行。
      scroller.scrollTop += Math.max(popoverBottom - bottom, field.getBoundingClientRect().top - top);
    }
  }, [topicOpen, filteredTopics.length, topicsLoading, topicsError]);

  useLayoutEffect(() => {
    if (!topicOpen) return;
    const list = topicListRef.current;
    const option = list?.children[topicCursor] as HTMLElement | undefined;
    if (!list || !option) return;
    const listBounds = list.getBoundingClientRect();
    const optionBounds = option.getBoundingClientRect();
    if (optionBounds.top < listBounds.top) list.scrollTop += optionBounds.top - listBounds.top;
    else if (optionBounds.bottom > listBounds.bottom) list.scrollTop += optionBounds.bottom - listBounds.bottom;
  }, [topicOpen, topicCursor, filteredTopics.length]);

  useEffect(() => {
    if (!topicOpen) return;
    const closeOnOutsideClick = (event: PointerEvent) => {
      if (event.target instanceof Node && !topicComboRef.current?.contains(event.target)) setTopicOpen(false);
    };
    document.addEventListener("pointerdown", closeOnOutsideClick);
    return () => document.removeEventListener("pointerdown", closeOnOutsideClick);
  }, [topicOpen]);

  const update = <K extends keyof typeof defaults>(name: K, value: (typeof defaults)[K]) => {
    if (["bootstrap", "securityProtocol", "saslMechanism", "saslUsername", "saslPassword"].includes(name)) setManagedClusterId("");
    setForm((previous) => ({ ...previous, [name]: value }));
  };
  const editConfig = (value: string) => { configDrafts.current[wantedConfigKey] = value; setConfigText(value); };
  useEffect(() => {
    let cancelled = false;
    setConfigSchemaKey(""); setConfigError("");
    void simApi<{ options: ConfigOption[] }>(`/config-schema?client=${form.client}&version=${encodeURIComponent(form.version)}&kind=${kind}`)
      .then(({ options }) => {
        if (cancelled) return;
        setConfigOptions(options);
        setConfigText(configDrafts.current[wantedConfigKey] ?? configTemplate(options, form.client, form.version, kind));
        setConfigSchemaKey(wantedConfigKey);
      }).catch((cause) => { if (!cancelled) setConfigError(cause instanceof Error ? cause.message : String(cause)); });
    return () => { cancelled = true; };
  }, [form.client, form.version, kind, wantedConfigKey]);
  const updateLimit = (name: LimitField, value: string) => {
    const next = editLimits(limitValues, limitDrivers, name, value, autoBalance && kind === "producer");
    setLimitDrivers(next.drivers);
    setForm((previous) => ({ ...previous, ...next.values }));
  };
  const toggleAutoBalance = () => {
    const next = !autoBalance;
    setAutoBalance(next);
    if (next) setForm((previous) => ({ ...previous, ...balanceLimits(previous, limitDrivers) }));
  };

  const refreshRuns = useCallback(async () => {
    const result = await simApi<{ runs: Run[] }>("/runs");
    setRuns(result.runs.filter((item) => item.kind === kind));
  }, [kind]);

  const loadTopics = useCallback(async () => {
    if (!form.bootstrap.trim()) { setTopicsError("先填写 Bootstrap Servers"); return; }
    if (form.securityProtocol.startsWith("SASL_") && (!form.saslUsername || (!managedClusterId && !form.saslPassword))) {
      setTopicsError("先填写 SASL 用户名和密码"); return;
    }
    const requestId = ++topicRequest.current;
    setTopicsLoading(true); setTopicsError("");
    try {
      const result = await simApi<{ topics: string[] }>("/topics/list", { method: "POST", body: JSON.stringify({
        clusterId: managedClusterId, bootstrap: form.bootstrap.trim(), securityProtocol: form.securityProtocol,
        saslMechanism: form.saslMechanism, saslUsername: form.saslUsername,
        saslPassword: form.saslPassword, caPath: form.caPath,
      }) });
      if (requestId === topicRequest.current) { setTopics(result.topics); setTopicsLoaded(true); setTopicCursor(0); }
    } catch (cause) {
      if (requestId === topicRequest.current) {
        setTopics([]); setTopicsLoaded(false);
        setTopicsError(cause instanceof Error ? cause.message : String(cause));
      }
    } finally { if (requestId === topicRequest.current) setTopicsLoading(false); }
  }, [managedClusterId, form.bootstrap, form.securityProtocol, form.saslMechanism, form.saslUsername, form.saslPassword, form.caPath]);

  useEffect(() => {
    topicRequest.current += 1;
    setTopics([]); setTopicsLoaded(false); setTopicsError(""); setTopicCursor(0);
  }, [loadTopics]);

  useEffect(() => {
    if (topicOpen && !topicsLoaded && form.bootstrap.trim()) {
      const timer = window.setTimeout(() => { void loadTopics(); }, 450);
      return () => clearTimeout(timer);
    }
  }, [loadTopics, topicOpen, topicsLoaded, form.bootstrap]);

  useEffect(() => {
    window.scrollTo(0, 0);
    setSelectedId(""); setDetail(null); setLogs([]); setRuns([]); cursor.current = 0; setError("");
    setForm((previous) => ({ ...previous, partition: "", commit: false }));
    void simApi<{ clients: Client[] }>("/catalog").then((result) => {
      setCatalog(result.clients);
      const preferred = result.clients.find((item) => item.versions.some((slot) => slot.available));
      const version = preferred?.versions.filter((slot) => slot.available).slice(-1)[0]?.version;
      if (preferred && version) setForm((previous) => ({ ...previous, client: preferred.id, version }));
      setCatalogLoaded(true);
    }).catch((cause) => { setCatalogLoaded(true); setError(cause instanceof Error ? cause.message : String(cause)); });
    void refreshRuns().catch((cause) => setError(cause instanceof Error ? cause.message : String(cause)));
  }, [kind, refreshRuns]);

  useEffect(() => {
    if (!selectedId) return;
    cursor.current = 0; setLogs([]); setDetail(null);
    let cancelled = false;
    const load = async () => {
      if (loadingDetail.current) return;
      loadingDetail.current = true;
      try {
        const [run, lines] = await Promise.all([
          simApi<Run>(`/runs/${selectedId}`),
          simApi<{ entries: LogEntry[]; next: number }>(`/runs/${selectedId}/logs?after=${cursor.current}`),
        ]);
        if (cancelled) return;
        setDetail(run);
        if (lines.entries.length) {
          cursor.current = lines.next;
          setLogs((previous) => retainLogWindow(previous, lines.entries));
        }
      } catch (cause) { if (!cancelled) setError(cause instanceof Error ? cause.message : String(cause)); }
      finally { loadingDetail.current = false; }
    };
    void load();
    const timer = window.setInterval(() => { void load(); }, 1200);
    return () => { cancelled = true; clearInterval(timer); };
  }, [selectedId]);

  useEffect(() => {
    void refreshRuns().catch(() => undefined);
    const timer = window.setInterval(() => { void refreshRuns().catch(() => undefined); }, 3000);
    return () => clearInterval(timer);
  }, [refreshRuns]);

  useEffect(() => { if (consoleRef.current) consoleRef.current.scrollTop = logView === "raw" ? 0 : consoleRef.current.scrollHeight; }, [logView]);
  useEffect(() => { if (logView === "events" && consoleRef.current) consoleRef.current.scrollTop = consoleRef.current.scrollHeight; }, [logs, logView]);

  const start = async () => {
    if (!form.bootstrap.trim() || !form.topic.trim() || !versionReady || mode !== "full" || limitsProblem || topicConfigProblem || advancedProblem) return;
    setBusy(true); setError("");
    try {
      const request = { ...form, kind, clientConfig: parsedConfig.values,
        count: Number(form.count), durationSeconds: Number(form.durationSeconds),
        rate: kind === "producer" ? Number(form.rate) : 1,
        connectionTimeoutMs: Number(form.connectionTimeoutMs), requestTimeoutMs: Number(form.requestTimeoutMs),
        maxMessageBytes: Number(form.maxMessageBytes), retries: Number(form.retries),
        maxFetchBytes: Number(form.maxFetchBytes), fetchMaxWaitMs: Number(form.fetchMaxWaitMs),
        partition: form.partition.trim() ? Number(form.partition) : null,
        bootstrap: form.bootstrap.trim(), topic: form.topic.trim(),
        autoCreateTopic: kind === "producer" && (!topicsLoaded || !topicExists), topicPartitions: Number(topicPartitions),
        topicReplicationFactor: Number(topicReplicationFactor) };
      const run = await simApi<Run>("/runs", { method: "POST", body: JSON.stringify(request) });
      setSelectedId(run.id);
      await refreshRuns();
    } catch (cause) { setError(cause instanceof Error ? cause.message : String(cause)); }
    finally { setBusy(false); }
  };

  const stop = async () => {
    if (!detail || mode !== "full") return;
    setBusy(true); setError("");
    try { await simApi(`/runs/${detail.id}/stop`, { method: "POST" }); await refreshRuns(); }
    catch (cause) { setError(cause instanceof Error ? cause.message : String(cause)); }
    finally { setBusy(false); }
  };

  const downloadRawLogs = async () => {
    if (!detail) return;
    setError("");
    try {
      const response = await apiFetch(`${API}/api/client-sim/runs/${detail.id}/logs/raw`, { headers: authHeaders() });
      if (response.status === 404) throw new Error("当前后端未加载原始日志下载接口，或运行记录已失效。请重启后端并刷新页面。");
      if (!response.ok) throw new Error(`下载原始日志失败 (${response.status})`);
      const url = URL.createObjectURL(await response.blob());
      const link = document.createElement("a");
      link.href = url;
      link.download = `${detail.id}-raw.log`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch (cause) { setError(cause instanceof Error ? cause.message : String(cause)); }
  };

  const importCluster = () => {
    if (!hasSelectedCluster) return;
    setForm((previous) => ({ ...previous, bootstrap: conn.bootstrap, securityProtocol: conn.securityProtocol,
      saslMechanism: conn.saslMechanism, saslUsername: conn.saslUsername, saslPassword: "" }));
    setManagedClusterId(conn.id);
  };

  const eventLogs = logs.filter((line) => line.source === "adapter" || line.source === "system" ||
    (!line.source && (Boolean(line.event) || line.channel === "system" || (line.channel === "stderr" && line.raw === undefined))));
  const rawLogs = logs.filter((line) => line.raw !== undefined && (line.source === "client" ||
    (!line.source && !line.event && (line.channel === "stdout" || line.channel === "stderr"))));
  const visibleLogs = eventLogs.filter((line) => logFilter === "all" ||
    (logFilter === "errors" ? isErrorLog(line) : line.event === logFilter));

  return <div className="sim-page">
    <PageHeader title={kind === "producer" ? "生产端模拟" : "消费端模拟"}
      right={<span className="sim-local-tag"><i />本地进程</span>} />
    {error && <div className="sim-alert" role="alert"><span>{error}</span><button type="button" onClick={() => setError("")} aria-label="关闭提示">×</button></div>}
    <div className="sim-layout">
      <section className="sim-config card" aria-label="模拟配置">
        <div className="sim-section-head"><h2>运行配置</h2><button ref={configButtonRef} type="button" className="sim-config-open" onClick={() => setConfigOpen(true)}>客户端配置 <span className="mono">{configOptions.length}</span><svg viewBox="0 0 16 16" fill="none" aria-hidden="true"><path d="m6 4 4 4-4 4" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" /></svg></button></div>
        <div ref={configScrollRef} className="sim-config-scroll scroll-box">
        <div className="sim-client-grid">
          {catalog.map((client) => <button type="button" key={client.id}
            className={`sim-client-choice ${form.client === client.id ? "is-selected" : ""}`}
            onClick={() => { setForm((previous) => ({ ...previous, client: client.id,
              partition: kind === "consumer" && client.id === "node" ? "" : previous.partition,
              version: client.versions.filter((slot) => slot.available).slice(-1)[0]?.version ?? client.versions.slice(-1)[0]?.version ?? "" })); }}>
            <span className="sim-client-mark">{client.id === "python" ? "Py" : client.id === "java" ? "Jv" : client.id === "node" ? "Js" : "Go"}</span>
            <span><b>{LANG[client.id]}</b><small>{client.versions.filter((slot) => slot.available).length} 个可用版本</small></span>
          </button>)}
        </div>
        <div className="sim-form-row sim-runtime-row">
          <label><span className="lbl">客户端版本</span><select className="field" value={chosen ? form.version : ""} disabled={!chosen} onChange={(event) => update("version", event.target.value)}>
            {!chosen && <option value="">{catalogLoaded ? "客户端列表不可用" : "正在加载客户端…"}</option>}
            {(chosen?.versions ?? []).map((slot) => <option key={slot.version} value={slot.version}>{slot.version}{slot.available ? " · 已就绪" : " · 未安装"}</option>)}
          </select></label>
          <label><span className="lbl">安全协议</span><select className="field" value={form.securityProtocol} onChange={(event) => update("securityProtocol", event.target.value)}>
            {(["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] as const).map((value) => <option key={value}>{value}</option>)}
          </select></label>
          <label><span className="lbl">客户端日志</span><select className="field" value={form.logLevel} onChange={(event) => update("logLevel", event.target.value)}>
            <option value="info">常规</option><option value="debug">详细</option>
          </select></label>
        </div>
        {catalogLoaded && catalog.length > 0 && !versionReady && <p className="sim-install-note">该版本尚未准备好。请在 KAFLAB 主机运行 <code>python backend/client_runners/setup.py {form.client} {form.version}</code> 后刷新页面。</p>}
        <div className="sim-divider" />
        <div className="sim-section-head"><h2>连接目标</h2><span>02 / TARGET</span></div>
        {!clustersLoading && !hasSelectedCluster && <div className="sim-target-hint" role="status"><span className="sim-target-hint-dot" />未选集群，请手动填写连接地址和 Topic</div>}
        <label><span className="lbl">Bootstrap Servers</span><input className="field mono" value={form.bootstrap} onChange={(event) => update("bootstrap", event.target.value)} placeholder="broker1:9092,broker2:9092" /></label>
        {hasSelectedCluster && <div className="sim-field-footer"><button type="button" className="sim-inline-action" onClick={importCluster}>从当前工作空间带入连接配置 ↗</button></div>}
        <div ref={topicFieldRef} className="sim-topic-field"><span className="lbl" id="sim-topic-label">Topic</span>
          <div ref={topicComboRef} className={`sim-topic-combo ${topicOpen ? "is-open" : ""}`} onBlur={(event) => { if (!event.currentTarget.contains(event.relatedTarget)) setTopicOpen(false); }}>
            <div className="sim-topic-input-wrap"><input className="field" role="combobox" aria-labelledby="sim-topic-label" aria-expanded={topicOpen} aria-controls="sim-topic-options"
              aria-autocomplete="list" aria-activedescendant={topicOpen && filteredTopics.length ? `sim-topic-option-${topicCursor}` : undefined}
              value={form.topic} onFocus={() => setTopicOpen(true)} onClick={() => setTopicOpen(true)}
              onChange={(event) => { update("topic", event.target.value); setTopicCursor(0); setTopicOpen(true); }}
              onKeyDown={(event) => {
                if (event.key === "ArrowDown") { event.preventDefault(); setTopicOpen(true); setTopicCursor((index) => Math.min(index + 1, Math.max(0, filteredTopics.length - 1))); }
                if (event.key === "ArrowUp") { event.preventDefault(); setTopicCursor((index) => Math.max(index - 1, 0)); }
                if (event.key === "Enter" && topicOpen && filteredTopics.length) { event.preventDefault(); update("topic", filteredTopics[topicCursor] ?? filteredTopics[0]); setTopicOpen(false); }
                if (event.key === "Escape") setTopicOpen(false);
              }} placeholder="搜索或输入 Topic 名称" />
              <button type="button" className="sim-topic-trigger" aria-label="查看 Topic 列表" aria-expanded={topicOpen}
                aria-controls="sim-topic-options" onClick={() => setTopicOpen((open) => !open)}>
                <svg className={`sim-topic-chevron${topicOpen ? " is-open" : ""}`} viewBox="0 0 16 16" fill="none" aria-hidden="true">
                  <path d="m4 6 4 4 4-4" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
              </button></div>
            {topicOpen && <div ref={topicPopoverRef} className="sim-topic-popover" id="sim-topic-options" role="listbox" onMouseDown={(event) => event.preventDefault()}>
              <div className="sim-topic-popover-head"><span>{topicsLoading ? "正在读取 Topic…" : topicsLoaded ? `${filteredTopics.length} 个匹配 Topic` : "目标集群 Topic"}</span><button type="button" onClick={() => void loadTopics()} disabled={topicsLoading}>刷新</button></div>
              {topicsError && <div className="sim-topic-message is-error">{topicsError}</div>}
              {!topicsError && topicsLoaded && filteredTopics.length === 0 && <div className="sim-topic-message">没有匹配项，可直接输入新名称</div>}
              <div ref={topicListRef} className="sim-topic-list">{filteredTopics.map((name, index) => <button id={`sim-topic-option-${index}`} type="button" role="option" aria-selected={name === form.topic} key={name}
                className={index === topicCursor ? "is-active" : ""}
                onMouseDown={(event) => event.preventDefault()} onClick={() => { update("topic", name); setTopicOpen(false); }}><span>{name}</span><small>已有</small></button>)}</div>
            </div>}
          </div>
          {newTopic && <div className="sim-topic-create"><span>启动时若不存在，将创建 <b>{topicName}</b></span><div><label>分区 <input type="number" min="1" max="1000" value={topicPartitions} onChange={(event) => setTopicPartitions(event.target.value)} /></label><label>副本 <input type="number" min="1" max="100" value={topicReplicationFactor} onChange={(event) => setTopicReplicationFactor(event.target.value)} /></label></div></div>}
          {topicConfigProblem && <div className="sim-limit-error" role="status">{topicConfigProblem}</div>}
        </div>
        {form.securityProtocol.startsWith("SASL_") && <div className="sim-form-row sim-auth-row">
          <label><span className="lbl">认证机制</span><select className="field" value={form.saslMechanism} onChange={(event) => update("saslMechanism", event.target.value)}>
            <option>PLAIN</option><option>SCRAM-SHA-256</option><option>SCRAM-SHA-512</option>
          </select></label>
          <label><span className="lbl">用户名</span><input className="field" value={form.saslUsername} onChange={(event) => update("saslUsername", event.target.value)} /></label>
          <label><span className="lbl">密码</span><input type="password" className="field" value={form.saslPassword} onChange={(event) => update("saslPassword", event.target.value)} autoComplete="off" /></label>
        </div>}
        {(form.securityProtocol === "SSL" || form.securityProtocol === "SASL_SSL") && <label><span className="lbl">CA 文件路径（可选，KAFLAB 主机）</span><input className="field mono" value={form.caPath} onChange={(event) => update("caPath", event.target.value)} placeholder="/path/to/ca.pem" /></label>}
        <div className="sim-divider" />
        <div className="sim-section-head"><h2>{kind === "producer" ? "发送场景" : "读取场景"}</h2><span>03 / SCENARIO</span></div>
        {kind === "producer" ? <>
          <div className="sim-form-row"><label><span className="lbl">消息 Key</span><input className="field" value={form.key} onChange={(event) => update("key", event.target.value)} placeholder="可留空" /></label>
            <label><span className="lbl">分区（可选）</span><input className="field nospin" type="number" min="0" value={form.partition} onChange={(event) => update("partition", event.target.value)} placeholder="自动" /></label></div>
          <label><span className="lbl">消息内容</span><textarea className="field sim-payload" value={form.value} onChange={(event) => update("value", event.target.value)} /></label>
        </> : <>
          <div className="sim-form-row"><label><span className="lbl">消费组 ID</span><input className="field" value={form.groupId} onChange={(event) => update("groupId", event.target.value)} placeholder="留空则自动生成独立消费组" disabled={form.partition !== ""} /></label>
            <label><span className="lbl">起始位置</span><select className="field" value={form.offsetStart} onChange={(event) => update("offsetStart", event.target.value)}><option value="latest">最新</option><option value="earliest">最早</option></select></label></div>
          <div className="sim-form-row"><label><span className="lbl">指定分区（可选）</span><input className="field nospin" type="number" min="0" value={form.partition} onChange={(event) => update("partition", event.target.value)} placeholder="消费组订阅" disabled={form.client === "node"} /></label>
            <label className="sim-check"><input type="checkbox" checked={form.commit} onChange={(event) => update("commit", event.target.checked)} disabled={form.partition !== ""} /><span>允许提交 Offset</span></label></div>
        </>}
        {kind === "producer" && <div className="sim-limit-toolbar"><span>{autoBalance ? "" : ""}</span><button type="button" className={`sim-auto-toggle ${autoBalance ? "is-on" : ""}`} aria-pressed={autoBalance} onClick={toggleAutoBalance}><i />自动平衡</button></div>}
        <div className="sim-form-row sim-limits"><label><span className="lbl">最多消息{kind === "producer" && autoBalance && derivedLimit === "count" && <em className="sim-derived-tag">自动</em>}</span><input className="field nospin" type="number" min="1" max="1000000" step="1" value={form.count} onChange={(event) => updateLimit("count", event.target.value)} /></label>
          <label><span className="lbl">最长运行（秒）{kind === "producer" && autoBalance && derivedLimit === "durationSeconds" && <em className="sim-derived-tag">自动</em>}</span><input className="field nospin" type="number" min="1" max="86400" step="1" value={form.durationSeconds} onChange={(event) => updateLimit("durationSeconds", event.target.value)} /></label>
          {kind === "producer" && <label><span className="lbl">速率（条/秒）{autoBalance && derivedLimit === "rate" && <em className="sim-derived-tag">自动</em>}</span><input className="field nospin" type="number" min="0.000001" max="1000" step="any" value={form.rate} onChange={(event) => updateLimit("rate", event.target.value)} /></label>}</div>
        {limitsProblem && <div className="sim-limit-error" role="status">{limitsProblem}</div>}
        </div>
        {advancedProblem && <div className="sim-limit-error" role="status">{advancedProblem}</div>}
        <div className="sim-actions"><button type="button" className="btn btn-primary" disabled={mode !== "full" || busy || !versionReady || Boolean(limitsProblem) || Boolean(topicConfigProblem) || Boolean(advancedProblem) || !form.bootstrap.trim() || !form.topic.trim() || (form.client === "node" && form.partition !== "" && kind === "consumer")}
          onClick={() => void start()}>{busy ? "处理中…" : kind === "producer" ? "启动生产模拟" : "启动消费模拟"}</button>
          {mode !== "full" && <span>切换到全部功能后可启动</span>}</div>
      </section>
      <div className="sim-observe">
        <section className="sim-console card" aria-label="运行详情">
          <div className="sim-section-head"><h2>运行详情</h2>{detail && <span className={`sim-status is-${detail.status}`}>{STATUS[detail.status] ?? detail.status}</span>}</div>
          {detail ? <>
            <div className="sim-run-heading"><div><b>{LANG[detail.client]} <span className="mono">{detail.version}</span></b><small className="mono">{detail.id} · PID {detail.pid ?? "—"}</small></div>
              <div className="sim-run-actions">{active && <button type="button" className="btn btn-danger-ghost btn-sm" disabled={busy || mode !== "full" || detail.status === "stopping"} onClick={() => void stop()}>停止进程</button>}</div></div>
            <div className="sim-run-stats"><div><small>{kind === "producer" ? "已确认" : "已读取"}</small><b>{kind === "producer" ? detail.sent : detail.received}</b></div><div><small>错误事件</small><b>{detail.errors}</b></div><div><small>退出码</small><b>{detail.exitCode ?? "—"}</b></div></div>
            {detail.error && <div className="sim-inline-error">{detail.error}</div>}
            <div className="sim-log-toolbar"><strong>运行日志</strong><div className="sim-log-tabs" role="tablist" aria-label="日志视图">
              <button type="button" role="tab" aria-selected={logView === "events"} className={logView === "events" ? "is-active" : ""} onClick={() => setLogView("events")}>事件</button>
              <button type="button" role="tab" aria-selected={logView === "raw"} className={logView === "raw" ? "is-active" : ""} onClick={() => setLogView("raw")}>原始输出</button>
            </div><div className="sim-log-tools">{logView === "events" ? <select aria-label="筛选日志" className="field" value={logFilter} onChange={(event) => setLogFilter(event.target.value)}>
              <option value="all">全部事件</option><option value="errors">仅错误</option><option value="sent">投递确认</option><option value="received">收到消息</option>
            </select> : <button type="button" className="sim-log-download" onClick={() => void downloadRawLogs()}>下载 .log</button>}</div></div>
            <div className="sim-log-view" ref={consoleRef} role="log" aria-live="off">
              {logView === "raw" && logs.length > 0 && !logs.some((entry) => entry.raw !== undefined) && <div className="sim-log-legacy">这条记录未保存进程原始行。请重启后端，并重新运行模拟以获取原始输出。</div>}
              {logView === "raw" && detail.client === "node" && detail.config.logLevel === "info" && detail.sent > 0 && rawLogs.length <= 1 &&
                <div className="sim-log-legacy">KafkaJS 常规级别通常没有成功发送明细；投递确认见“事件”。下次运行选“详细”可查看客户端调试日志。</div>}
              {cursor.current > logs.length && <div className="sim-log-legacy">页面保留起始与最新日志；下载 .log 可查看全部进程原始输出。</div>}
              {(logView === "raw" ? rawLogs : visibleLogs).length ? (logView === "raw" ? rawLogs : visibleLogs).map((entry) => <div key={entry.seq} className={`sim-log-line ${logView === "raw" ? "is-raw" : ""} ${isErrorLog(entry) ? "is-error" : ""}`}>
                <time dateTime={entry.time}>{localTime(entry.time, logClock)}</time><span className="sim-log-channel">{logView === "raw" ? entry.channel : entry.event || entry.channel}</span><span>{logView === "raw" ? entry.raw ?? entry.message : entry.message}{logView === "raw" && entry.rawTruncated && <em> [单行已截断]</em>}{logView === "events" && entry.partition !== undefined && <em> P{entry.partition}{entry.offset !== undefined ? ` · #${entry.offset}` : ""}</em>}</span>
              </div>) : <div className="sim-log-empty">{logView === "raw" ? "进程尚无原始输出，可在下次运行前选择详细日志" : logs.length ? "当前筛选下没有事件" : "正在等待进程输出…"}</div>}
            </div>{detail.logTruncated && <div className="sim-log-limit">日志已达到保存上限，后续输出不再保存。</div>}
          </> : <div className="sim-console-empty"><span className="sim-empty-glyph">›_</span><b>暂无运行记录</b></div>}
        </section>
        <section className="sim-history card" aria-label="运行记录"><div className="sim-section-head"><h2>最近运行</h2><span>{runs.length} 条</span></div>
          <div className="sim-history-list">{runs.length ? runs.map((run) => <button type="button" key={run.id} className={`sim-history-row ${selectedId === run.id ? "is-selected" : ""}`} onClick={() => setSelectedId(run.id)}>
            <span className={`sim-history-dot is-${run.status}`} /><span className="sim-history-main"><b>{LANG[run.client]} <span className="mono">{run.version}</span></b><small>{run.config.topic} · {run.config.bootstrap}</small></span>
            <span className="sim-history-meta"><b className={`sim-status is-${run.status}`}>{STATUS[run.status] ?? run.status}</b><small>{localTime(run.createdAt, runClock)}</small></span>
          </button>) : <div className="sim-history-empty">尚无运行记录</div>}</div>
        </section>
      </div>
    </div>
    {configOpen && <div className="sim-drawer-backdrop" onMouseDown={(event) => { if (event.target === event.currentTarget) setConfigOpen(false); }}>
      <aside ref={configDrawerRef} className="sim-config-drawer" role="dialog" aria-modal="true" aria-labelledby="sim-config-drawer-title">
        <div className="sim-drawer-head"><div><h2 id="sim-config-drawer-title">客户端配置</h2><p>{LANG[form.client]} {form.version} · {kind === "producer" ? "生产端" : "消费端"}参数</p></div>
          <button type="button" className="sim-drawer-close" aria-label="关闭客户端配置" onClick={() => setConfigOpen(false)} autoFocus>×</button></div>
        <div className="sim-drawer-body scroll-box">
          <div className="sim-config-file">
            <div className="sim-config-file-head"><div><b>配置文件</b></div>
              <button type="button" onClick={() => editConfig(configTemplate(configOptions, form.client, form.version, kind))} disabled={configSchemaKey !== wantedConfigKey}>恢复默认</button></div>
            <div className="sim-config-editor"><div className="sim-config-editor-bar"><span><i /> client.config</span><em>{configOptions.length} 项参数</em></div>
              <textarea spellCheck={false} aria-label="客户端配置文件" value={configText} onChange={(event) => editConfig(event.target.value)} disabled={configSchemaKey !== wantedConfigKey} /></div>
            <p className="sim-config-file-note">{kind === "producer" ? "大消息需同时调整 sim.max.message.bytes、客户端请求/批次上限及 Broker 限制。" : "拉取大小是请求目标值；完整批次可能超过该值。"}</p>
          </div>
          {advancedProblem && <div className="sim-limit-error" role="status">{advancedProblem}</div>}
        </div>
        <div className="sim-drawer-foot"><span>{advancedProblem ? "配置需要修正" : `${configOptions.length} 项参数 · 已同步到运行配置`}</span><button type="button" className="btn btn-primary btn-sm" onClick={() => setConfigOpen(false)}>完成</button></div>
      </aside>
    </div>}
  </div>;
}
