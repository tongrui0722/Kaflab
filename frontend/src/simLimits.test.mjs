import test from "node:test";
import assert from "node:assert/strict";
import { balanceLimits, editLimits, limitError } from "./simLimits.ts";

test("消息数和时长自动计算速率，且计算结果仍能手动修改", () => {
  const initial = { count: "100", durationSeconds: "100", rate: "1" };
  const changed = editLimits(initial, ["durationSeconds", "count"], "rate", "2", true);
  assert.deepEqual(changed.drivers, ["count", "rate"]);
  assert.equal(changed.values.durationSeconds, "50");
  assert.equal(balanceLimits(initial, ["count", "durationSeconds"]).rate, "1");
});

test("最近编辑的两项决定第三项；关闭自动平衡后互不覆盖", () => {
  const values = { count: "100", durationSeconds: "100", rate: "1" };
  const byDuration = editLimits(values, ["count", "rate"], "durationSeconds", "40", true);
  assert.equal(byDuration.values.count, "40");
  const manual = editLimits(values, ["count", "rate"], "durationSeconds", "40", false);
  assert.equal(manual.values.count, "100");
  assert.equal(balanceLimits(manual.values, manual.drivers).count, "40");
});

test("输入过程中的空值不被覆盖；大于旧上限的合法值可提交", () => {
  const values = { count: "100000", durationSeconds: "6000", rate: "5" };
  assert.equal(balanceLimits(values, ["count", "durationSeconds"]).rate, "16.6667");
  assert.equal(limitError(values, true), "");
  const editing = editLimits(values, ["durationSeconds", "count"], "count", "", true);
  assert.equal(editing.values.rate, "5");
  assert.match(limitError(editing.values, true), /最多消息/);
});
