// @ts-check
import { expect, test } from "vitest";

import { defaultExecutorBus } from "@/js/adhoc-executor-bus.js";

// Regression pins for the "permanent Refreshing… spinner when hiding the wizard" bug.
//
// AdhocGridControls renders a Refresh button whose v-if reads `executorBus.isQueryInFlight`
// (and a label that branches on `executorBus.isSameAsLastQuery`). A previous version used
// four separate inject tokens, each defaulting to `{ value: false }` — a *truthy object* in
// templates, which forced the v-if branch to always render and picked the "Refreshing…"
// label. The button only appeared when the wizard was hidden, which is the exact symptom the
// user reported. Pin the shape here so a future "let's add a value wrapper" tweak fails fast.

test("defaultExecutorBus returns a fresh object each call (no shared mutable singleton)", () => {
	const a = defaultExecutorBus();
	const b = defaultExecutorBus();
	expect(a).not.toBe(b);
	a.isQueryInFlight = true;
	expect(b.isQueryInFlight).toBe(false);
});

test("defaultExecutorBus: all rendering flags are plain falsy booleans (not truthy objects)", () => {
	const bus = defaultExecutorBus();

	// These three are read by `v-if` / ternaries in adhoc-query-grid-controls.js — they MUST
	// be plain booleans so the spinner branch stays hidden when no executor has populated the
	// bus. Asserting both `=== false` AND `!!flag === false` makes the intent obvious: a
	// `{value: false}` regression would pass `=== {value: false}` but flunk the !! check.
	expect(bus.isQueryInFlight).toBe(false);
	expect(!!bus.isQueryInFlight).toBe(false);

	expect(bus.isSameAsLastQuery).toBe(false);
	expect(!!bus.isSameAsLastQuery).toBe(false);

	expect(bus.autoQuery).toBe(false);
	expect(!!bus.autoQuery).toBe(false);
});

test("defaultExecutorBus.submitQuery: callable no-op (clicking Refresh outside the executor scope must not throw)", () => {
	const bus = defaultExecutorBus();
	expect(typeof bus.submitQuery).toBe("function");
	expect(() => bus.submitQuery()).not.toThrow();
});
