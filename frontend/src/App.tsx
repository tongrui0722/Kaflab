import { useEffect, useState } from "react";
import { HashRouter, Routes, Route, Navigate } from "react-router-dom";
import { ConnProvider } from "./conn";
import { LabProvider } from "./lab";
import { AuthProvider, LoginPage, useAuth } from "./auth";
import { Shell, ConnBar, type ConnState } from "./components/Shell";
import { ProducerPage } from "./pages/Producer";
import { ConsumerPage } from "./pages/Consumer";
import { GroupPage } from "./pages/Group";
import { ClustersPage } from "./pages/Clusters";
import { TopicsPage } from "./pages/Topics";
import { TopicDetailPage } from "./pages/TopicDetail";
import { DashboardPage } from "./pages/Dashboard";
import { ProtocolPage, FaultsPage, TracePage, SettingsPage } from "./pages/Misc";
import { LoadTestPage } from "./pages/LoadTest";
import { Placeholder } from "./components/Shell";

function Guarded() {
  const { token, mode } = useAuth();
  const [conn, setConn] = useState<ConnState>({ ok: null, bootstrap: "192.168.127.129:19092" });
  useEffect(() => {
    const h = (e: Event) => setConn((e as CustomEvent<ConnState>).detail);
    window.addEventListener("kcl-conn", h);
    return () => window.removeEventListener("kcl-conn", h);
  }, []);
  if (!token) return <LoginPage />;
  const readonly = mode === "readonly";
  const locked = (title: string) => (
    <Placeholder title={title} note="只读模式下该功能已隐藏，用【全部功能】模式登录后可用。" />
  );
  return (
    <ConnProvider>
    <LabProvider>
    <HashRouter>
      <Shell status={<ConnBar conn={conn} />}>
        <Routes>
          <Route path="/" element={<Navigate to="/dashboard" replace />} />
          <Route path="/dashboard" element={<DashboardPage />} />
          <Route path="/clusters" element={<ClustersPage />} />
          <Route path="/topics" element={<TopicsPage />} />
          <Route path="/topics/:name" element={<TopicDetailPage />} />
          <Route path="/producer" element={readonly ? locked("Producer") : <ProducerPage />} />
          <Route path="/consumer" element={<ConsumerPage />} />
          <Route path="/group" element={<GroupPage />} />
          <Route path="/protocol" element={<ProtocolPage />} />
          <Route path="/faults" element={readonly ? locked("故障模拟") : <FaultsPage />} />
          <Route path="/loadtest" element={readonly ? locked("压测") : <LoadTestPage />} />
          <Route path="/trace" element={<TracePage />} />
          <Route path="/settings" element={<SettingsPage />} />
          <Route path="*" element={<Placeholder title="未知路由" note="返回 Producer。" />} />
        </Routes>
      </Shell>
    </HashRouter>
    </LabProvider>
    </ConnProvider>
  );
}

export default function App() {
  return (
    <AuthProvider>
      <Guarded />
    </AuthProvider>
  );
}

// Producer 上报连接态（避免 prop drilling，用自定义事件）
export function reportConn(c: ConnState) {
  window.dispatchEvent(new CustomEvent("kcl-conn", { detail: c }));
}
