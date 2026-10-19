export type Broker = { id?: number; host: string; port: number; leaders?: number; replicas?: number; isr?: number };

export function BrokerDistribution({ brokers, controllerId, hasDistribution, loading = false }: {
  brokers: Broker[]; controllerId?: number | null; hasDistribution: boolean; loading?: boolean;
}) {
  const maxLeaders = Math.max(0, ...brokers.map((broker) => broker.leaders ?? 0));

  return <section className="card overview-panel overview-broker-panel">
    <div className="overview-panel-head">
      <h2 className="overview-panel-title">Broker 分区分布</h2>
      <span className="badge mute">{brokers.length} 在线节点</span>
    </div>
    {loading ? <div className="overview-empty">等待集群快照…</div>
      : brokers.length === 0 ? <div className="overview-empty">Broker 元数据不可用</div>
      : <div className={`overview-broker-list scroll-box${brokers.length > 3 ? " is-scrollable" : ""}`}>
        {brokers.map((broker) => <div key={broker.id ?? `${broker.host}:${broker.port}`} className="overview-broker-item">
          <div className="overview-broker-row">
            <span className="overview-broker-id mono">#{broker.id ?? "?"}</span>
            <span className="overview-broker-address">
              <span className="overview-broker-host mono" title={`${broker.host}:${broker.port}`}>{broker.host}:{broker.port}</span>
              {broker.id === controllerId && <span className="overview-controller-badge" title="当前集群 Controller">Controller</span>}
            </span>
            {hasDistribution && <span className="overview-broker-leaders"><b className="mono">{broker.leaders ?? 0}</b><small>Leader</small></span>}
          </div>
          {hasDistribution && <>
            <div className="overview-bar-track" aria-hidden="true"><div className="overview-bar-fill" style={{ width: `${(broker.leaders ?? 0) > 0 ? Math.max(2, ((broker.leaders ?? 0) / maxLeaders) * 100) : 0}%` }} /></div>
            <div className="overview-broker-meta"><span>副本 <b className="mono">{broker.replicas ?? "—"}</b></span><span>ISR <b className="mono">{broker.isr ?? "—"}</b></span></div>
          </>}
        </div>)}
      </div>}
  </section>;
}
