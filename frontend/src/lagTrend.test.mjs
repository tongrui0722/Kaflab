import assert from "node:assert/strict";
import { test } from "node:test";
import { normalizeLagPoints } from "./lagTrend.ts";

test("same-second duplicate samples do not inflate cluster Lag", () => {
  const persisted = [
    { series: "group-a", ts: 100, value: 32000 },
    { series: "group-b", ts: 100, value: 35996 },
    { series: "group-a", ts: 100, value: 32000 },
    { series: "group-b", ts: 100, value: 35996 },
    { series: "__total__", ts: 100, value: 135992 },
  ];
  assert.deepEqual(normalizeLagPoints(persisted, [], 0), [
    { series: "__total__", ts: 100, value: 67996 },
    { series: "group-a", ts: 100, value: 32000 },
    { series: "group-b", ts: 100, value: 35996 },
  ]);
});

test("current snapshot wins at the same second and invalid samples are discarded", () => {
  const persisted = [
    { series: "group-a", ts: 10, value: 99 },
    { series: "group-a", ts: 100, value: 30 },
    { series: "group-b", ts: 100, value: Number.NaN },
  ];
  const live = [
    { series: "group-a", ts: 100, value: 12 },
    { series: "group-b", ts: 100, value: -2 },
  ];
  assert.deepEqual(normalizeLagPoints(persisted, live, 50), [
    { series: "__total__", ts: 100, value: 12 },
    { series: "group-a", ts: 100, value: 12 },
    { series: "group-b", ts: 100, value: 0 },
  ]);
});

test("a zero-group total is preserved when no group samples exist", () => {
  assert.deepEqual(normalizeLagPoints([{ series: "__total__", ts: 100, value: 0 }], [], 0), [
    { series: "__total__", ts: 100, value: 0 },
  ]);
});
