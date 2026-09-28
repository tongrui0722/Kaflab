import { createContext, useCallback, useContext, useRef, useState, type ReactNode } from "react";
import { useAuth } from "./auth";

interface LabCtx {
  unlocked: boolean;
  /** 写入动作统一入口：未解锁先弹确认框，确认后执行；已解锁直接执行 */
  requireLab: (fn: () => void) => void;
  lock: () => void;
}

const Ctx = createContext<LabCtx>({ unlocked: false, requireLab: (fn) => fn(), lock: () => undefined });

const KEY = "kcl-lab-unlocked";

export function LabProvider({ children }: { children: ReactNode }) {
  const { mode } = useAuth();
  const [unlocked, setUnlocked] = useState(() => {
    try { return sessionStorage.getItem(KEY) === "1"; } catch { return false; }
  });
  const [asking, setAsking] = useState(false);
  const pending = useRef<(() => void) | null>(null);

  const requireLab = useCallback((fn: () => void) => {
    if (mode === "full") { fn(); return; }  // 全部功能模式：直接放行
    let open = false;
    try { open = sessionStorage.getItem(KEY) === "1"; } catch { /* ignore */ }
    if (open) { fn(); return; }
    pending.current = fn;
    setAsking(true);
  }, [mode]);

  const confirm = () => {
    try { sessionStorage.setItem(KEY, "1"); } catch { /* ignore */ }
    setUnlocked(true);
    setAsking(false);
    pending.current?.();
    pending.current = null;
  };
  const cancel = () => {
    setAsking(false);
    pending.current = null;
  };
  const lock = () => {
    try { sessionStorage.removeItem(KEY); } catch { /* ignore */ }
    setUnlocked(false);
  };

  return (
    <Ctx.Provider value={{ unlocked, requireLab, lock }}>
      {children}
      {asking && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4" onClick={cancel}>
          <div className="card w-full max-w-md p-5" onClick={(e) => e.stopPropagation()}>
            <div className="text-[14px] font-bold text-slate-100">进入实验模式？</div>
            <div className="mt-2 text-[12.5px] leading-relaxed text-slate-400">
              解锁后可执行写入操作：发送消息、压测、入组演示、位点提交、故障注入，
              都会对当前集群产生<span className="font-semibold text-amber-300">真实影响</span>。
              查询功能不受影响。本次浏览器会话有效，关闭标签页自动锁定。
            </div>
            <div className="mt-4 flex justify-end gap-2">
              <button onClick={cancel} className="btn btn-ghost btn-sm">取消</button>
              <button onClick={confirm} className="btn btn-warn btn-sm">确认解锁并继续</button>
            </div>
          </div>
        </div>
      )}
    </Ctx.Provider>
  );
}

export function useLab() {
  return useContext(Ctx);
}
