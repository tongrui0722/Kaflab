export function retainLogWindow<T extends { seq: number }>(previous: T[], incoming: T[], head = 100, tail = 600): T[] {
  const seen = new Set(previous.map((entry) => entry.seq));
  const merged = [...previous];
  for (const entry of incoming) {
    if (seen.has(entry.seq)) continue;
    seen.add(entry.seq);
    merged.push(entry);
  }
  return merged.length > head + tail
    ? [...merged.slice(0, head), ...merged.slice(-tail)] : merged;
}
