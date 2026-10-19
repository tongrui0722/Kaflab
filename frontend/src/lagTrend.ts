export type LagPoint = { series: string; value: number; ts: number };

export function normalizeLagPoints(persisted: LagPoint[], live: LagPoint[], cutoff: number): LagPoint[] {
  const groups = new Map<string, LagPoint>();
  const legacyTotals = new Map<number, LagPoint>();
  // The history endpoint can contain several writes for the same group in one
  // second. Keep one value per group/timestamp before deriving the cluster sum.
  [...persisted, ...live].forEach((point) => {
    if (!Number.isFinite(point.ts) || !Number.isFinite(point.value) || point.ts < cutoff) return;
    const clean = { ...point, value: Math.max(0, point.value) };
    if (point.series === "__total__") legacyTotals.set(point.ts, clean);
    else if (point.series) groups.set(`${point.ts}:${point.series}`, clean);
  });
  const totals = new Map<number, number>();
  groups.forEach((point) => totals.set(point.ts, (totals.get(point.ts) ?? 0) + point.value));
  const totalPoints = [...totals].map(([ts, value]) => ({ series: "__total__", ts, value }));
  legacyTotals.forEach((point, ts) => { if (!totals.has(ts)) totalPoints.push(point); });
  return [...groups.values(), ...totalPoints].sort((a, b) => a.ts - b.ts || a.series.localeCompare(b.series));
}
