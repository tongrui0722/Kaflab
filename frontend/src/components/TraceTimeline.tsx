import type { TraceStep } from "../types";

const ICON: Record<number, string> = { 1: "⇄", 2: "⛁", 3: "❖", 4: "◈", 5: "⬢", 6: "⬣", 7: "✔" };

export function TraceTimeline({ steps, selected, onSelect, total }:
  { steps: TraceStep[]; selected: number; onSelect: (i: number) => void; total?: string }) {
  return (
    <div>
      <div className="card-t">
        <span className="flex h-6 w-6 items-center justify-center rounded-lg bg-[rgba(56,189,248,0.14)] text-[11px] text-sky-700">▶</span>
        <span className="text-[13px] font-bold text-slate-800">Producer</span>
        <span className="text-[11px] text-[var(--text-faint)]">完整链路</span>
        <span className="ml-auto flex items-center gap-1.5">
          <span className="badge ok">成功</span>
          {total && <span className="mono text-[11px] text-slate-400">{total}</span>}
        </span>
      </div>
      <ol className="relative ml-[15px] space-y-[3px] border-l border-[var(--border)]">
        {steps.map((s) => {
          const active = s.index === selected;
          const dot =
            s.status === "success" ? "bg-emerald-400 text-[#052e22]" :
            s.status === "error" ? "bg-rose-400 text-[#3b0a12]" : "bg-sky-400 text-[#08222e] pulsing";
          const glyph = s.status === "error" ? "✖" : s.status === "pending" ? "…" : (ICON[s.index] ?? s.index);
          const errText = s.status === "error" && s.event
            ? (s.event.errorName ? `${s.event.errorName}${s.event.errorCode != null ? ` (${s.event.errorCode})` : ""}` : "ERROR")
            : null;
          return (
            <li key={s.index}>
              <button onClick={() => onSelect(s.index)} aria-current={active ? "step" : undefined}
                className={`trace-step-button relative flex w-full items-center gap-3 rounded-lg px-2 py-[7px] text-left ${active ? "is-active" : ""}`}>
                <span className={`absolute -left-[15px] flex h-[26px] w-[26px] items-center justify-center rounded-full text-[12px] font-bold ring-4 ring-[var(--panel)] ${dot}`} title={errText ?? s.title}>{glyph}</span>
                <span className="ml-5 min-w-0 flex-1">
                  <span className="flex items-baseline gap-2">
                    <span className="mono text-[11px] text-[var(--text-faint)]">{s.index}</span>
                    <span className={`truncate text-[12.5px] font-semibold ${s.status === "error" ? "text-rose-700" : "text-slate-800"}`}>{s.title}</span>
                    {errText && <span className="mono truncate text-[11px] text-rose-600">{errText}</span>}
                  </span>
                  <span className="mono ml-5 block truncate text-[11px] text-[var(--text-faint)]">{s.desc}</span>
                </span>
                <span className="mono shrink-0 text-[11px] text-emerald-700/90">{s.latency}</span>
              </button>
            </li>
          );
        })}
      </ol>
      <div className="mt-2 flex items-center justify-between border-t border-[var(--border-soft)] px-2 pt-2 text-[12px]">
        <span className="text-[var(--text-faint)]">Total</span>
        <span className="mono font-bold text-emerald-700">{total ?? "—"}</span>
      </div>
    </div>
  );
}
