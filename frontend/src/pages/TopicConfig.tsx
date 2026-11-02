import { apiFetch, apiWebSocket } from "../api/http";
import { useEffect, useMemo, useState } from "react";
import { authHeaders } from "../auth";

type ConfigItem = {
  name: string;
  value: string | null;
  defaultValue?: string | null;
  effectiveValue?: string | null;
  isDefault?: boolean;
  isSensitive?: boolean;
  readOnly?: boolean;
  type?: string;
  documentation?: string;
};

type ConfigData = {
  topic: string;
  configs: ConfigItem[];
  editable?: string[];
  readonly?: string[];
  sensitive?: string[];
};

function catOf(name: string): string {
  if (name.startsWith("retention") || name.startsWith("segment") || name === "cleanup.policy") return "保留与清理";
  if (name.startsWith("max.") || name.startsWith("min") || name === "compression.type") return "大小与压缩";
  if (name.includes("leader") || name.includes("replica") || name.includes("isr") || name.includes("unclean")) return "副本与容错";
  if (name.includes("flush") || name.includes("index") || name.includes("file") || name.includes("preallocate")) return "存储与刷盘";
  if (name.includes("delete") || name.includes("compact")) return "删除与压缩";
  return "其他";
}

function typeOf(c: ConfigItem): string {
  if (c.type) return c.type;
  if (/^(true|false)$/i.test(c.effectiveValue ?? c.value ?? c.defaultValue ?? "")) return "BOOLEAN";
  return "STRING";
}

function fmtVal(v: string | null | undefined): string {
  if (v == null || v === "") return "—";
  if (/^-?\d+$/.test(v)) {
    const n = Number(v);
    if (Math.abs(n) >= 86400000 && Math.abs(n) % 86400000 === 0) return `${v}（${n / 86400000} 天）`;
    if (Math.abs(n) >= 3600000 && Math.abs(n) % 3600000 === 0) return `${v}（${n / 3600000} 小时）`;
    if (Math.abs(n) >= 60000 && Math.abs(n) % 60000 === 0) return `${v}（${n / 60000} 分钟）`;
    if (Math.abs(n) >= 1048576 && Math.abs(n) % 1048576 === 0) return `${v}（${n / 1048576} MB）`;
    return Number(v).toLocaleString("zh-CN");
  }
  return v;
}

function riskOf(name: string, next: string | null): string | null {
  if (next == null) return null;
  if (name === "cleanup.policy" && next.includes("compact")) return "压缩会改变保留语义，确认消费端可处理重复 Key";
  if (name === "retention.ms" && Number(next) < 86400000) return "保留小于 1 天，历史消息会被快速删除";
  if (name === "retention.bytes" && Number(next) < 1073741824) return "容量上限较小，可能频繁触发段删除";
  if (name === "max.message.bytes" && Number(next) > 10485760) return "单条上限较大，注意 Broker 与消费端内存";
  if (name === "unclean.leader.election.enable" && /^true$/i.test(next)) return "允许非 ISR 副本当选，可能丢消息";
  if (name === "min.insync.replicas" && Number(next) <= 1) return "ISR 确认数过低，可用性换 durability，请确认";
  return null;
}

const COMMON = [
  "cleanup.policy", "retention.ms", "retention.bytes", "segment.ms",
  "segment.bytes", "max.message.bytes", "compression.type",
  "min.insync.replicas", "unclean.leader.election.enable",
];

export function TopicConfigPanel({ cid, topic, readonly }: { cid: string; topic: string; readonly?: boolean }) {
  const [data, setData] = useState<ConfigData | null>(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);
  const [q, setQ] = useState("");
  const [scope, setScope] = useState<"common" | "all">("common");
  const [cat, setCat] = useState("all");
  const [draft, setDraft] = useState<Record<string, string | undefined>>({});
  const [confirm, setConfirm] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveErr, setSaveErr] = useState<string | null>(null);
  const [saveOk, setSaveOk] = useState<string | null>(null);

  const load = async () => {
    setLoading(true);
    setErr(null);
    try {
      const r = await apiFetch(
        `${(import.meta.env.VITE_API_BASE as string) || window.location.origin}/api/clusters/${cid}/topics/${encodeURIComponent(topic)}/config`,
        { headers: authHeaders() });
      const body = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(body?.detail || `读取失败 (${r.status})`);
      setData(body as ConfigData);
      setDraft({});
      setSaveOk(null);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { void load(); /* eslint-disable-next-line react-hooks/exhaustive-deps */ }, [cid, topic]);

  const cats = useMemo(() => {
    const s = new Set((data?.configs ?? []).map((c) => catOf(c.name)));
    return ["all", ...[...s].sort()];
  }, [data]);

  const rows = useMemo(() => {
    let list = data?.configs ?? [];
    if (scope === "common") list = list.filter((c) => COMMON.includes(c.name));
    if (cat !== "all") list = list.filter((c) => catOf(c.name) === cat);
    const kw = q.trim().toLowerCase();
    if (kw) list = list.filter((c) => c.name.toLowerCase().includes(kw)
      || (c.documentation ?? "").toLowerCase().includes(kw));
    return [...list].sort((a, b) => a.name.localeCompare(b.name));
  }, [data, scope, cat, q]);

  const changed = useMemo(() => {
    if (!data) return [];
    return (data.configs ?? []).map((c) => {
      const d = draft[c.name];
      if (d === undefined) return null;
      const eff = c.effectiveValue ?? c.value ?? "";
      if (d === (c.value ?? "")) return null;
      return { name: c.name, from: eff || "—", to: d === "" ? "恢复默认" : d, reset: d === "", risk: riskOf(c.name, d === "" ? null : d) };
    }).filter(Boolean) as { name: string; from: string; to: string; reset: boolean; risk: string | null }[];
  }, [data, draft]);

  const save = async () => {
    const configs: Record<string, string | null> = {};
    changed.forEach((ch) => { configs[ch.name] = ch.reset ? null : (draft[ch.name] ?? ""); });
    setSaving(true);
    setSaveErr(null);
    try {
      const r = await apiFetch(
        `${(import.meta.env.VITE_API_BASE as string) || window.location.origin}/api/clusters/${cid}/topics/${encodeURIComponent(topic)}/config`,
        {
          method: "PUT",
          headers: { "Content-Type": "application/json", ...authHeaders() },
          body: JSON.stringify({ configs, keys: changed.map((ch) => ch.name), preview: false }),
        });
      const body = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(body?.detail || `保存失败 (${r.status})`);
      setConfirm(false);
      await load();
      setSaveOk(`已保存 ${changed.length} 项，实际生效值已重新读取`);
    } catch (e) {
      setSaveErr(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  if (loading) return <div className="py-10 text-center text-[12px] text-slate-500">正在读取 Topic 配置…</div>;
  if (err || !data) {
    const noAuth = /401|403/.test(err ?? "");
    return (
      <div className="py-10 text-center">
        <div className="text-[13px] text-slate-600">{noAuth ? "无权限查看该 Topic 配置" : "配置读取失败"}</div>
        {err && !noAuth && <div className="mono mt-1 text-[11px] text-slate-500">{err}</div>}
        <button onClick={() => void load()} className="btn btn-ghost btn-sm mt-3">重试</button>
      </div>
    );
  }

  return (
    <div>
      {readonly && <div className="alert info">只读模式：仅可查看配置，用【全部功能】模式登录后可修改。</div>}
      <div className="topic-config-toolbar">
        <div className="seg">
          {(["common", "all"] as const).map((s) => (
            <button key={s} onClick={() => setScope(s)} className={scope === s ? "on" : ""}>
              {s === "common" ? "常用配置" : "全部配置"}
            </button>
          ))}
        </div>
        <select value={cat} onChange={(e) => setCat(e.target.value)} className="field topic-config-category">
          {cats.map((c) => <option key={c} value={c}>{c === "all" ? "全部分类" : c}</option>)}
        </select>
        <input aria-label="搜索 Topic 配置" value={q} onChange={(e) => setQ(e.target.value)} placeholder="搜索配置名或说明…"
          className="field mono min-w-[160px] flex-1 !text-[12px]" />
        <span className="badge mute">{rows.length} / {data.configs.length}</span>
        <button onClick={() => void load()} className="btn btn-ghost btn-sm">重新读取</button>
      </div>

      {saveOk && <div className="alert ok mt-3">{saveOk}</div>}
      {saveErr && <div className="alert err mt-3">保存失败：{saveErr}（已编辑内容已保留，可重试）</div>}

      {rows.length === 0 ? (
        <div className="py-10 text-center text-[12px] text-slate-500">
          {data.configs.length === 0 ? "Kafka 未返回任何配置（可能无权限或版本不支持）。" : "没有匹配的配置，换个关键词试试。"}
        </div>
      ) : (
        <div className="tbl-wrap topic-config-table-wrap">
          <table className="tbl min-w-[720px]">
            <thead className="sticky top-0 z-10 bg-[var(--panel-2)]">
              <tr>
                <th>配置</th><th>当前生效值</th><th>默认值</th><th>来源</th><th className="!text-right">修改</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((c) => {
                const d = draft[c.name];
                const changedRow = d !== undefined && d !== (c.value ?? "");
                const effValue = c.effectiveValue ?? c.value ?? c.defaultValue ?? "";
                const src = (c.value ?? "") !== "" ? "Topic 覆盖" : "集群默认";
                const editable = !c.readOnly;
                return (
                  <tr key={c.name} className={`transition-colors hover:bg-slate-900/[0.04] ${changedRow ? "bg-[rgba(251,191,36,0.04)]" : ""}`}>
                    <td className="max-w-[260px]">
                      <span className="mono text-[12px] text-slate-700" title={c.documentation || c.name}>{c.name}</span>
                      <div className="mt-0.5 flex flex-wrap gap-1">
                        <span className="badge mute">{typeOf(c)}</span>
                        {catOf(c.name) !== "其他" && <span className="badge vio">{catOf(c.name)}</span>}
                        {c.isSensitive && <span className="badge warn">敏感</span>}
                      </div>
                    </td>
                    <td className="mono max-w-[220px] break-all text-[12px]">
                      {c.isSensitive && !changedRow ? <span className="text-slate-500">••••••</span>
                      : <span className={changedRow ? "text-amber-700 line-through" : "text-slate-700"}>{fmtVal(effValue)}</span>}
                      {changedRow && <div className="mt-0.5 font-semibold text-sky-700">{d === "" ? "恢复默认" : fmtVal(d)}</div>}
                    </td>
                    <td className="mono max-w-[180px] break-all text-[12px] text-slate-500">{fmtVal(c.defaultValue)}</td>
                    <td><span className={`badge ${(c.value ?? "") !== "" ? "run" : "mute"}`}>{src}</span></td>
                    <td className="!text-right">
                      {!editable ? <span className="text-[11px] text-slate-500">只读</span>
                      : typeOf(c) === "BOOLEAN" ? (
                        <select value={d === undefined ? (c.value ?? "") : (d ?? "")} disabled={readonly} title={readonly ? "只读模式下不能修改配置" : undefined}
                          onChange={(e) => setDraft((p) => ({ ...p, [c.name]: e.target.value }))}
                          className="field !h-[30px] !w-[130px] !text-[12px]">
                          <option value="">恢复默认</option>
                          <option value="true">true</option>
                          <option value="false">false</option>
                        </select>
                      ) : (
                        <input value={d === undefined ? (c.value ?? "") : (d ?? "")} disabled={readonly} title={readonly ? "只读模式下不能修改配置" : undefined}
                          onChange={(e) => setDraft((p) => ({ ...p, [c.name]: e.target.value }))}
                          placeholder="留空=恢复默认"
                          className="field mono !h-[30px] !w-[170px] !text-[12px]" />
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {changed.length > 0 && !confirm && (
        <div className="topic-config-changes">
          <span className="text-[12px] font-semibold text-amber-800">已修改 {changed.length} 项</span>
          <span className="ml-auto flex gap-2">
            <button onClick={() => setDraft({})} className="btn btn-ghost btn-sm">放弃修改</button>
            <button onClick={() => setConfirm(true)} disabled={readonly} className="btn btn-primary btn-sm">保存</button>
          </span>
        </div>
      )}
      {confirm && (
        <div role="dialog" aria-modal="true" aria-label="确认 Topic 配置变更" className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4" onClick={() => !saving && setConfirm(false)}>
          <div className="card w-full max-w-lg p-4" onClick={(e) => e.stopPropagation()}>
            <div className="text-[13px] font-bold text-slate-700">确认保存以下 {changed.length} 项变更？</div>
            <div className="mt-2 max-h-[300px] space-y-1.5 overflow-y-auto">
              {changed.map((ch) => (
                <div key={ch.name} className="rounded-lg border border-[var(--border-soft)] px-2.5 py-1.5">
                  <div className="mono text-[12px] text-slate-700">{ch.name}</div>
                  <div className="mono mt-0.5 break-all text-[11px] text-slate-500">
                    {ch.from} <span className="mx-1">→</span>
                    <span className="font-semibold text-slate-700">{ch.to}</span>
                  </div>
                  {ch.risk && <div className="alert warn mt-1 !py-1">{ch.risk}</div>}
                </div>
              ))}
            </div>
            {saveErr && <div className="alert err mt-2">保存失败：{saveErr}</div>}
            <div className="mt-3 flex justify-end gap-2">
              <button onClick={() => setConfirm(false)} disabled={saving} className="btn btn-ghost btn-sm">取消</button>
              <button onClick={() => void save()} disabled={saving || readonly} title={readonly ? "只读模式下禁止保存配置" : undefined} className="btn btn-primary btn-sm">
                {saving ? "保存中…" : "确认保存"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
