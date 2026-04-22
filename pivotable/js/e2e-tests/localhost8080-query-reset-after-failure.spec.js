import { test, expect } from "@playwright/test";

import queryPivotable from "./query-pivotable.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// Regression test for the "Reset query" button failing to recover after a broken query.
//
// Repro reported by the user: on the `simple` cube, build `city + delta`, then add the
// `always_throws` measure (an ICombination wired to throw on every slice). The grid error
// banner pops up. Click the `Reset query` button — the expected behaviour is to land on an
// empty view (no columns, no measures, no banner); observed behaviour was that the UI
// remained stuck on the broken state.
test("Reset query recovers the UI after a failing query on simple cube", async ({ page }) => {
	await page.goto(url);

	// Login via the non-modal path — same mechanics as queryPivotable.queryPivotable() but
	// targeting the `simple` cube instead of WorldCupPlayers.
	await page.getByRole("link", { name: /You need to login/ }).click();
	await page.getByRole("link", { name: "pivotable-unsafe_fakeuser" }).click();
	await page.getByRole("button", { name: "Login fakeUser" }).click();

	await page.getByRole("link", { name: "Browse through endpoints" }).click();
	// The `simple` cube is registered on the localhost self-endpoint.
	await page
		.getByRole("link", { name: /simple/i })
		.first()
		.click();
	await page.getByRole("link", { name: /Query simple/i }).click();

	// ── Build a working query: city + delta ──
	await queryPivotable.addColumn(page, "city");
	// addMeasure toggles the JSON switch off; pass a measure name that exists on the simple cube.
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("delta");
	await page.getByRole("switch", { name: "JSON" }).uncheck();
	await page.getByRole("button", { name: /measures/ }).click();
	await page.getByRole("switch", { name: /^delta$/ }).check();

	// Sanity: at least one row rendered — i.e. the query ran successfully.
	await expect(page.locator(".slick-row").first()).toBeVisible();

	// ── Break the query by adding the always-throwing measure ──
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("always_throws");
	await page.getByRole("switch", { name: /^always_throws$/ }).check();

	// The error banner above the grid must surface the failure.
	const errorBanner = page.getByText(/Query is broken/);
	await expect(errorBanner).toBeVisible();

	// Wizard confirms the faulty measure is selected (repro of the pre-fix state).
	await expect(page.getByRole("switch", { name: /^always_throws$/ })).toBeChecked();

	// ── Click Reset query ──
	await page.getByRole("button", { name: /Reset query/ }).click();

	// EXPECTED: the banner disappears. Pre-fix, the empty-query that followed the reset was
	// rejected by the backend and re-populated `tabularView.error`, leaving the banner up.
	await expect(errorBanner).toBeHidden();

	// EXPECTED: the grid is empty — no data rows rendered.
	await expect(page.locator(".slick-row")).toHaveCount(0);

	// EXPECTED: the wizard reflects the reset — `always_throws` unchecked in-place.
	// Pre-fix, `queryModel.reset()` reassigned `selectedMeasures = {}` which broke
	// per-key reactivity for `v-model` bindings that had already resolved to the prior
	// proxy, leaving the switch visually stuck on "checked" even though the selection
	// was logically dropped. The fix clears keys in place via `delete`.
	await expect(page.getByRole("switch", { name: /^always_throws$/ })).not.toBeChecked();
	await expect(page.getByRole("switch", { name: /^delta$/ })).not.toBeChecked();
});
