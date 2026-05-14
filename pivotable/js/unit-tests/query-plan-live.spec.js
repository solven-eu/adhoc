// @ts-check
import { expect, test } from "vitest";

import { nextPollState } from "@/js/adhoc-query-plan-live.js";

// Pure reducer tests — no timers, no fetch, no DOM. Covers each transition in the four-state contract
// (200 / 204+Retry-After / 204 / 404) plus the transient error path.

const PENDING_SUMMARY = {
	state: "PENDING",
	totalNodes: 4,
	doneNodes: 0,
	pendingNodes: 4,
	runningNodes: 0,
	failedNodes: 0,
	elapsedMs: 0,
	startDelayMs: 100,
	totalRowsOut: 0,
	latestCompletedLabel: null,
};
const RUNNING_SUMMARY = { ...PENDING_SUMMARY, state: "RUNNING", doneNodes: 2, runningNodes: 1, pendingNodes: 1 };
const DONE_SUMMARY = { ...PENDING_SUMMARY, state: "DONE", doneNodes: 4, pendingNodes: 0 };
const FAILED_SUMMARY = { ...PENDING_SUMMARY, state: "FAILED", doneNodes: 3, failedNodes: 1, pendingNodes: 0 };

test("404/unknown stops the poller — the UUID was never seen by Pivotable", () => {
	const prev = { summary: RUNNING_SUMMARY, status: /** @type {const} */ ("polling") };
	const outcome = nextPollState(prev, "unknown");
	expect(outcome.shouldStop).toBe(true);
	expect(outcome.state.status).toBe("unknown");
});

test("204+Retry-After (queuing) keeps polling — engine is still planning the query", () => {
	const prev = { summary: null, status: /** @type {const} */ ("polling") };
	const outcome = nextPollState(prev, "queuing");
	expect(outcome.shouldStop).toBe(false);
	expect(outcome.state.status).toBe("queuing");
	expect(outcome.state.summary).toBeNull();
});

test("204 without Retry-After (gone) stops the poller — plan was evicted post-termination", () => {
	const prev = { summary: DONE_SUMMARY, status: /** @type {const} */ ("polling") };
	const outcome = nextPollState(prev, "gone");
	expect(outcome.shouldStop).toBe(true);
	expect(outcome.state.status).toBe("gone");
	expect(outcome.state.summary).toBe(DONE_SUMMARY); // preserve last-known so UI keeps showing it
});

test("error keeps polling and preserves the last summary (no flash to loading)", () => {
	const prev = { summary: RUNNING_SUMMARY, status: /** @type {const} */ ("polling") };
	const outcome = nextPollState(prev, "error");
	expect(outcome.shouldStop).toBe(false);
	expect(outcome.state.status).toBe("error");
	expect(outcome.state.summary).toBe(RUNNING_SUMMARY);
});

test("error before any successful summary keeps `summary: null` (preserves prev)", () => {
	const prev = { summary: null, status: /** @type {const} */ ("polling") };
	const outcome = nextPollState(prev, "error");
	expect(outcome.state.summary).toBeNull();
	expect(outcome.state.status).toBe("error");
});

test("mid-flight RUNNING summary stays in polling mode", () => {
	const prev = { summary: null, status: /** @type {const} */ ("polling") };
	const outcome = nextPollState(prev, RUNNING_SUMMARY);
	expect(outcome.shouldStop).toBe(false);
	expect(outcome.state.status).toBe("polling");
	expect(outcome.state.summary).toBe(RUNNING_SUMMARY);
});

test("PENDING (queued by the engine) summary stays in polling mode", () => {
	// Note: server-side PENDING is distinct from the SPA's `queuing` state. PENDING means the engine has
	// registered a plan but no node has started yet; `queuing` means the engine doesn't have a plan at all.
	const prev = { summary: null, status: /** @type {const} */ ("polling") };
	const outcome = nextPollState(prev, PENDING_SUMMARY);
	expect(outcome.shouldStop).toBe(false);
	expect(outcome.state.status).toBe("polling");
});

test("DONE summary flips to terminal and stops polling", () => {
	const prev = { summary: RUNNING_SUMMARY, status: /** @type {const} */ ("polling") };
	const outcome = nextPollState(prev, DONE_SUMMARY);
	expect(outcome.shouldStop).toBe(true);
	expect(outcome.state.status).toBe("terminal");
	expect(outcome.state.summary).toBe(DONE_SUMMARY);
});

test("FAILED summary also flips to terminal — both DONE and FAILED are terminal states", () => {
	const prev = { summary: RUNNING_SUMMARY, status: /** @type {const} */ ("polling") };
	const outcome = nextPollState(prev, FAILED_SUMMARY);
	expect(outcome.shouldStop).toBe(true);
	expect(outcome.state.status).toBe("terminal");
});

test("recovery: error → real summary returns to polling and adopts the new data", () => {
	const afterError = nextPollState({ summary: RUNNING_SUMMARY, status: "polling" }, "error");
	expect(afterError.state.status).toBe("error");

	const recovered = nextPollState(afterError.state, DONE_SUMMARY);
	expect(recovered.state.status).toBe("terminal");
	expect(recovered.state.summary).toBe(DONE_SUMMARY);
	expect(recovered.shouldStop).toBe(true);
});

test("transition: queuing → polling once the engine registers the first summary", () => {
	const afterQueuing = nextPollState({ summary: null, status: "polling" }, "queuing");
	expect(afterQueuing.state.status).toBe("queuing");
	expect(afterQueuing.shouldStop).toBe(false);

	const afterFirstSummary = nextPollState(afterQueuing.state, PENDING_SUMMARY);
	expect(afterFirstSummary.state.status).toBe("polling");
	expect(afterFirstSummary.state.summary).toBe(PENDING_SUMMARY);
});
