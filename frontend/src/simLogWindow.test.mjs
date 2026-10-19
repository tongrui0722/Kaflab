import test from "node:test";
import assert from "node:assert/strict";
import { retainLogWindow } from "./simLogWindow.ts";

test("verbose logs retain startup and newest entries without duplicates", () => {
  const first = retainLogWindow([], Array.from({ length: 1000 }, (_, index) => ({ seq: index + 1 })));
  assert.equal(first.length, 700);
  assert.equal(first[0].seq, 1);
  assert.equal(first[99].seq, 100);
  assert.equal(first[100].seq, 401);
  const next = retainLogWindow(first, Array.from({ length: 21 }, (_, index) => ({ seq: index + 1000 })));
  assert.equal(next[0].seq, 1);
  assert.equal(next.at(-1).seq, 1020);
  assert.equal(new Set(next.map((entry) => entry.seq)).size, next.length);
});
