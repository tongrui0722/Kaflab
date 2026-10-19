import { useEffect, useId, useMemo, useRef, useState, type KeyboardEvent } from "react";

interface TopicPickerProps {
  value: string;
  options: string[];
  loading?: boolean;
  error?: boolean;
  onChange: (value: string) => void;
  onRefresh?: () => void;
  placeholder?: string;
}

/** Producer 与 Consumer 共用的可输入 Topic 选择框。 */
export function TopicPicker({ value, options, loading = false, error = false, onChange, onRefresh,
  placeholder = "搜索或输入 Topic" }: TopicPickerProps) {
  const id = useId();
  const root = useRef<HTMLDivElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const matches = useMemo(() => {
    const query = value.trim().toLocaleLowerCase();
    return options.filter((topic) => topic.toLocaleLowerCase().includes(query));
  }, [options, value]);
  const visible = matches.slice(0, 100);
  const custom = value.trim() && !options.includes(value.trim()) ? value.trim() : "";
  const itemCount = loading || error ? 0 : visible.length + (custom ? 1 : 0);

  useEffect(() => { setActive(0); }, [value, options]);
  useEffect(() => {
    if (!open) return;
    const dismiss = (event: PointerEvent) => {
      if (root.current && !root.current.contains(event.target as Node)) setOpen(false);
    };
    document.addEventListener("pointerdown", dismiss);
    return () => document.removeEventListener("pointerdown", dismiss);
  }, [open]);

  const select = (topic: string) => {
    onChange(topic);
    setOpen(false);
    input.current?.focus();
  };
  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Escape") { setOpen(false); return; }
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      if (!open) { setOpen(true); onRefresh?.(); return; }
      if (itemCount) setActive((current) => (current + (event.key === "ArrowDown" ? 1 : -1) + itemCount) % itemCount);
    }
    if (event.key === "Enter" && open) {
      event.preventDefault();
      const selected = itemCount ? active < visible.length ? visible[active] : custom : "";
      if (selected) select(selected);
      else setOpen(false);
    }
  };

  return <div className="topic-picker" ref={root}>
    <label className="lbl" htmlFor={`${id}-input`}>Topic</label>
    <div className="topic-picker-control">
      <input ref={input} id={`${id}-input`} className="field mono" role="combobox"
        aria-autocomplete="list" aria-expanded={open} aria-controls={`${id}-list`}
        aria-activedescendant={open && itemCount ? `${id}-option-${active}` : undefined}
        value={value} placeholder={placeholder} autoComplete="off" spellCheck={false}
        onFocus={() => { setOpen(true); onRefresh?.(); }}
        onChange={(event) => { onChange(event.target.value); setOpen(true); }} onKeyDown={onKeyDown} />
      <button type="button" className="topic-picker-trigger" aria-label="查看 Topic 列表"
        aria-expanded={open} aria-controls={`${id}-list`}
        onClick={() => { if (open) setOpen(false); else { setOpen(true); input.current?.focus(); } }}>
        <svg viewBox="0 0 16 16" fill="none" aria-hidden="true"><path d="m4 6 4 4 4-4" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" /></svg>
      </button>
    </div>
    {open && <div id={`${id}-list`} className="topic-picker-list scroll-box" role="listbox" aria-label="Topic 列表">
      {loading ? <div className="topic-picker-message" role="status">正在读取 Topic…</div>
        : error ? <div className="topic-picker-message" role="status">Topic 列表加载失败
            {onRefresh && <button type="button" onMouseDown={(event) => event.preventDefault()} onClick={onRefresh}>重试</button>}</div>
          : <>
            {visible.map((topic, index) => <button type="button" role="option" id={`${id}-option-${index}`}
              aria-selected={index === active} key={topic} className={index === active ? "is-active" : ""}
              onMouseEnter={() => setActive(index)} onMouseDown={(event) => event.preventDefault()}
              onClick={() => select(topic)} title={topic}>{topic}</button>)}
            {custom && <button type="button" role="option" id={`${id}-option-${visible.length}`}
              aria-selected={active === visible.length} className={active === visible.length ? "is-active" : ""}
              onMouseEnter={() => setActive(visible.length)} onMouseDown={(event) => event.preventDefault()}
              onClick={() => select(custom)}>使用“{custom}”</button>}
            {!itemCount && <div className="topic-picker-message">暂无 Topic</div>}
            {matches.length > visible.length && <div className="topic-picker-message">仅显示前 100 项，请继续输入名称</div>}
          </>}
    </div>}
  </div>;
}
