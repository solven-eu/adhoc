// @ts-check
import { test, expect } from "@playwright/test";

import queryPivotable from "./query-pivotable.mjs";

// E2E coverage for the LiveView badge wired into adhoc-query.js. The fixture cube (WorldCupPlayers) returns in
// milliseconds, so this spec exercises the happy path end-to-end: submit an async query, watch the LiveView poll the
// /plan/summary endpoint, and assert the badge reaches the terminal `Done` state. The intermediate `queuing` /
// `running` states are too fast to observe deterministically on this fixture — covered by the Vitest reducer spec
// instead.

import { BASE_URL as url } from "./_url.mjs";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

test("LiveView badge surfaces a terminal Done state after an async query completes", async ({ page }) => {
	await page.goto(url);
	await queryPivotable.queryPivotable(page);

	// The badge sits above the grid; assert it eventually renders the success-state badge. The
	// `.adhoc-plan-live` root class is set on the component's wrapper div, so we anchor on that.
	const liveBadge = page.locator(".adhoc-plan-live");
	await expect(liveBadge).toBeVisible();
	await expect(liveBadge.locator(".badge.bg-success")).toHaveText("Done");

	// Step counts: the cube has 1+ DAG nodes, so we just assert the chip contains a "/X step" pattern.
	await expect(liveBadge).toContainText(/\d+\/\d+ step/);
});

test("LiveView badge is absent before any query has been submitted", async ({ page }) => {
	await page.goto(url);

	// Log in but do not submit a query. The component is gated on `tabularView.queryUuid`, which is unset until
	// the executor's async submission completes.
	await page.getByRole("link", { name: /You need to login/ }).click();
	await page.getByRole("link", { name: "pivotable-unsafe_fakeuser" }).click();
	await page.getByRole("button", { name: /^Login$/i }).click();
	await page.getByRole("link", { name: "Browse through endpoints" }).click();
	await page.getByRole("link", { name: /WorldCupPlayers/ }).click();
	await page.getByRole("link", { name: /Query WorldCupPlayers/ }).click();

	// No query submitted yet → badge MUST NOT be in the DOM.
	await expect(page.locator(".adhoc-plan-live")).toHaveCount(0);
});
