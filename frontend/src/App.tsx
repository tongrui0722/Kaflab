import { useEffect, useState, type ReactNode } from "react";
import { HashRouter, Routes, Route, Navigate } from "react-router-dom";
import { ConnProvider, useConn } from "./conn";
import { LabProvider } from "./lab";
import { AuthProvider, LoginPage, useAuth } from "./auth";
import { Shell, ConnBar, type ConnState } from "./components/Shell";
import { ProducerPage } from "./pages/Producer";
import { ConsumerPage } from "./pages/Consumer";
import { GroupPage } from "./pages/Group";
import { ClustersPage } from "./pages/Clusters";
import { TopicsPage } from "./pages/Topics";
import { TopicDetailPage } from "./pages/TopicDetail";
import { ClusterOverviewPage } from "./pages/ClusterOverview";
import { ClientSimulatorPage } from "./pages/ClientSimulator";
import { LoadTestPage } from "./pages/LoadTest";
import { OperationsPage } from "./pages/Operations";
import { MonitoringPage } from "./pages/Monitoring";
import { Placeholder } from "./components/Shell";

function Guarded() {
  const { token } = useAuth();
  const [conn, setConn] = useState<ConnState>({ ok: null, bootstrap: "192.168.127.129:19092" });
  useEffect(() => {
    const h = (e: Event) => setConn((e as CustomEvent<ConnState>).detail);
    window.addEventListener("kcl-conn", h);
    return () => window.removeEventListener("kcl-conn", h);
  }, []);
  if (!token) return <LoginPage />;
  return (
    <ConnProvider>
    <LabProvider>
    <HashRouter>
      <Shell status={<ConnBar conn={conn} />}>
        <Routes>
          <Route path="/" element={<Navigate to="/clusters" replace />} />
          <Route path="/dashboard" element={<RequireCluster><ClusterOverviewPage /></RequireCluster>} />
          <Route path="/monitoring" element={<RequireCluster><MonitoringPage /></RequireCluster>} />
          <Route path="/clusters" element={<ClustersPage />} />
          <Route path="/topics" element={<RequireCluster><TopicsPage /></RequireCluster>} />
          <Route path="/topics/:name" element={<RequireCluster><TopicDetailPage /></RequireCluster>} />
          <Route path="/producer" element={<RequireCluster><ProducerPage /></RequireCluster>} />
          <Route path="/consumer" element={<RequireCluster><ConsumerPage /></RequireCluster>} />
          <Route path="/group" element={<RequireCluster><GroupPage /></RequireCluster>} />
          <Route path="/sim/producer" element={<ClientSimulatorPage kind="producer" />} />
          <Route path="/sim/consumer" element={<ClientSimulatorPage kind="consumer" />} />
          <Route path="/loadtest" element={<LoadTestPage />} />
          <Route path="/operations" element={<OperationsPage />} />
          <Route path="*" element={<Placeholder title="未知路由" note="返回 Producer。" />} />
        </Routes>
      </Shell>
    </HashRouter>
    </LabProvider>
    </ConnProvider>
  );
}

function RequireCluster({ children }: { children: ReactNode }) {
  const { clusters, activeId, loading } = useConn();
  if (loading) return <div className="card p-6 text-center text-[12px] text-slate-500">正在加载集群…</div>;
  if (!activeId || !clusters.some((cluster) => cluster.id === activeId)) return <Navigate to="/clusters" replace />;
  return <>{children}</>;
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
