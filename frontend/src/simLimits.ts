export type LimitField = "count" | "durationSeconds" | "rate";
export type LimitValues = Record<LimitField, string>;
export type LimitDrivers = readonly [LimitField, LimitField];

const fields: LimitField[] = ["count", "durationSeconds", "rate"];

function validNumber(field: LimitField, raw: string): number | null {
  if (!raw.trim()) return null;
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) return null;
  if (field !== "rate" && !Number.isInteger(value)) return null;
  return value;
}

export function editLimits(values: LimitValues, drivers: LimitDrivers, changed: LimitField, raw: string, automatic: boolean) {
  const anchor = drivers.includes(changed) ? drivers.find((field) => field !== changed)! : drivers[1];
  const nextDrivers: LimitDrivers = [anchor, changed];
  const nextValues = { ...values, [changed]: raw };
  return { values: automatic ? balanceLimits(nextValues, nextDrivers) : nextValues, drivers: nextDrivers };
}

export function balanceLimits(values: LimitValues, drivers: LimitDrivers): LimitValues {
  const derived = fields.find((field) => !drivers.includes(field))!;
  const first = validNumber(drivers[0], values[drivers[0]]);
  const second = validNumber(drivers[1], values[drivers[1]]);
  if (first === null || second === null) return values;

  const count = Number(values.count);
  const duration = Number(values.durationSeconds);
  const rate = Number(values.rate);
  const result = derived === "rate" ? count / duration
    : derived === "durationSeconds" ? Math.ceil(count / rate)
      : Math.max(1, Math.round(duration * rate));
  if (!Number.isFinite(result) || result <= 0) return values;
  return { ...values, [derived]: String(derived === "rate" ? Number(result.toPrecision(6)) : result) };
}

export function limitError(values: LimitValues, producer: boolean): string {
  const count = validNumber("count", values.count);
  if (count === null || count > 1_000_000) return "最多消息应为 1–1,000,000 的整数";
  const duration = validNumber("durationSeconds", values.durationSeconds);
  if (duration === null || duration > 86_400) return "最长运行应为 1–86,400 秒的整数";
  if (producer) {
    const rate = validNumber("rate", values.rate);
    if (rate === null || rate > 1000) return "速率应大于 0 且不超过 1,000 条/秒";
  }
  return "";
}
