import { createContext, useCallback, useContext, type ReactNode } from "react";
import { useAuth } from "./auth";

interface LabCtx {
  unlocked: boolean;
  /** 只允许全部功能模式执行写入动作；只读模式直接拦截。 */
  requireLab: (fn: () => void) => void;
  lock: () => void;
}

const Ctx = createContext<LabCtx>({ unlocked: false, requireLab: (fn) => fn(), lock: () => undefined });

export function LabProvider({ children }: { children: ReactNode }) {
  const { mode } = useAuth();
  const unlocked = false;
  const requireLab = useCallback((fn: () => void) => {
    if (mode === "full") fn();
  }, [mode]);
  const lock = useCallback(() => undefined, []);

  return (
    <Ctx.Provider value={{ unlocked, requireLab, lock }}>
      {children}
    </Ctx.Provider>
  );
}

export function useLab() {
  return useContext(Ctx);
}
