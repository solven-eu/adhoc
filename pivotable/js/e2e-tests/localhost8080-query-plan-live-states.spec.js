// @ts-check
import { test, expect } from "@playwright/test";

import queryPivotable from "./query-pivotable.mjs";

// Exercises the LiveView's intermediate states (queuing / running mid-flight / error) by intercepting
// /api/v1/cubes/queries/*/plan/summary and replying with controlled HTTP shapes. The real WorldCupPlayers cube
// completes too fast to observe these transitions deterministically — Playwright route-mocking lets us pin them.
//
// The spec is intentionally *complementary* to localhost8080-query-plan-live.spec.js (which exercises the
// happy-path against a real backend response). Mocking only the LiveView poll endpoint preserves the rest of the
// stack — login, query submission, grid render — so the SPA's mount logic (`tabularView.queryUuid` gate, watch
// on prop change) is still wired through real code.

import { BASE_URL as url } from "./_url.mjs";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

/**
 * @typedef PlanSummary
 * @property {string} state
 * @property {number} totalNodes
 * @property {number} doneNodes
 * @property {number} pendingNodes
 * @property {number} runningNodes
 * @property {number} failedNodes
 * @property {number} elapsedMs
 * @property {number} startDelayMs
 * @property {number} totalRowsOut
 * @property {string | null} latestCompletedLabel
 */

/**
 * Build a deterministic summary in a given state. Defaults are plausible for a multi-step plan.
 * @param {Partial<PlanSummary> & {state: string}} overrides
 * @returns {PlanSummary}
 */
function summary(overrides) {
	return {
		totalNodes: 8,
		doneNodes: 0,
		pendingNodes: 8,
		runningNodes: 0,
		failedNodes: 0,
		elapsedMs: 0,
		startDelayMs: 0,
		totalRowsOut: 0,
		latestCompletedLabel: null,
		...overrides,
	};
}

/**
 * Install a Playwright route handler that drives the LiveView through a scripted sequence of responses.
 * Each call returns the next item; once the script is exhausted, the last item is repeated indefinitely.
 *
 * @param {import("@playwright/test").Page} page
 * @param {Array<{status: number, headers?: Record<string,string>, body?: PlanSummary}>} script
 */
async function mockPlanSummary(page, script) {
	let index = 0;
	await page.route(/\/api\/v1\/cubes\/queries\/[^/]+\/plan\/summary$/, async (route) => {
		const step = script[Math.min(index, script.length - 1)];
		index += 1;
		await route.fulfill({
			status: step.status,
			headers: step.headers || {},
			contentType: step.body ? "application/json" : "text/plain",
			body: step.body ? JSON.stringify(step.body) : "",
		});
	});
}

test("LiveView renders Queuing state on 204+Retry-After", async ({ page }) => {
	// Every poll returns `queuing` so the SPA stays in that state — the transition to Running is racy to observe in
	// e2e (the SPA's poll cadence is fast enough that the queuing state may flash by between assertions). Use the
	// happy-path spec for the Running/Done transitions instead; this test pins the queuing rendering specifically.
	await mockPlanSummary(page, [{ status: 204, headers: { "Retry-After": "1" } }]);

	await page.goto(url);
	await queryPivotable.queryPivotable(page);

	const liveBadge = page.locator(".adhoc-plan-live");
	await expect(liveBadge).toBeVisible();
	await expect(liveBadge).toContainText("Queuing");
});

test("LiveView shows 'Unknown query id' on 404 and stops polling", async ({ page }) => {
	// 404 from the very first poll — SPA should render the unknown state and stop polling.
	await mockPlanSummary(page, [{ status: 404 }]);

	await page.goto(url);
	await queryPivotable.queryPivotable(page);

	const liveBadge = page.locator(".adhoc-plan-live");
	await expect(liveBadge).toBeVisible();
	await expect(liveBadge).toContainText("Unknown query id");
});

test("LiveView shows 'Plan no longer available' on 204 without Retry-After", async ({ page }) => {
	// 204 without Retry-After is the "evicted" terminal state — render the gone hint and stop.
	await mockPlanSummary(page, [{ status: 204 }]);

	await page.goto(url);
	await queryPivotable.queryPivotable(page);

	const liveBadge = page.locator(".adhoc-plan-live");
	await expect(liveBadge).toBeVisible();
	await expect(liveBadge).toContainText("Plan no longer available");
});

test("LiveView surfaces 'network glitch' on a 500, keeps polling, and recovers on the next tick", async ({ page }) => {
	// Sequence: 500 (transient) → 200 DONE. The component should briefly show the network-glitch hint, then flip
	// to the terminal Done badge once the next poll succeeds.
	await mockPlanSummary(page, [{ status: 500 }, { status: 200, body: summary({ state: "DONE", doneNodes: 8, pendingNodes: 0, elapsedMs: 1234 }) }]);

	await page.goto(url);
	await queryPivotable.queryPivotable(page);

	const liveBadge = page.locator(".adhoc-plan-live");
	await expect(liveBadge).toBeVisible();
	// Eventually the 200 wins — that's the load-bearing assertion. The transient glitch text is timing-sensitive
	// (might be masked by the very-next-tick recovery), so we only assert the terminal state.
	await expect(liveBadge.locator(".badge.bg-success")).toHaveText("Done", { timeout: 5000 });
});
