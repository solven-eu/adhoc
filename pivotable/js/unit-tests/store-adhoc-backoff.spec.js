// @ts-check
import { expect, test } from "vitest";

import { backoffDelayMs } from "@/js/store-adhoc.js";

// Pure schedule — no fetch, no timers. The store action wraps this with state and `setTimeout`,
// but the policy itself is what we want to lock in: attempt 1 is the FIRST failure-retry pair,
// not "before any attempt".

test("attempt 1 → 2s (first retry after first failure)", () => {
	expect(backoffDelayMs(1)).toBe(2000);
});

test("attempt 2 → 5s", () => {
	expect(backoffDelayMs(2)).toBe(5000);
});

test("attempt 3 → 10s", () => {
	expect(backoffDelayMs(3)).toBe(10000);
});

test("attempt 4 → 30s", () => {
	expect(backoffDelayMs(4)).toBe(30000);
});

test("attempt 5 → 60s (cap)", () => {
	expect(backoffDelayMs(5)).toBe(60000);
});

test("attempts beyond 5 stay capped at 60s", () => {
	expect(backoffDelayMs(6)).toBe(60000);
	expect(backoffDelayMs(50)).toBe(60000);
	expect(backoffDelayMs(1000)).toBe(60000);
});

test("attempt 0 (defensive, should not happen) clamps to the first slot", () => {
	expect(backoffDelayMs(0)).toBe(2000);
});

test("negative attempt (defensive) clamps to the first slot", () => {
	expect(backoffDelayMs(-3)).toBe(2000);
});
