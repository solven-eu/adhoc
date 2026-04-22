import { test, expect } from "@playwright/test";

import queryPivotable from "./query-pivotable.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// Regression test for the "Restore last successful query" button in the query-broken banner.
//
// When a query fails (e.g. adding the `always_throws` measure), the grid keeps showing the
// last successful view and a sticky banner offers a Restore action. Clicking Restore must:
//   1. Rebuild the queryModel from the snapshot captured on the last successful response,
//      dropping the faulty measure.
//   2. Reflect the restored state in the WIZARD (left sidebar) — so the faulty measure's
//      switch flips back to unchecked, and any added columns that came after the last
//      success disappear.
//   3. Hide the banner.
//   4. Keep the grid populated (the previous successful view stays visible; optionally a
//      fresh re-query will land and update it).
//
// The original bug was (2): `queryModel.reset()` and `parsedJsonToQueryModel` were
// REASSIGNING fresh `{}` objects to `selectedMeasures` / `selectedColumns` / `filter`,
// which broke the reactive chain for `v-model` bindings that had already resolved to the
// prior proxy — leaving switches visually stuck. Fix: clear in-place (`delete key` +
// `Object.assign`) so per-key reactivity fires the template updates.
test("Restore last successful query updates the wizard switches", async ({ page }) => {
	await page.goto(url);

	await page.getByRole("link", { name: /You need to login/ }).click();
	await page.getByRole("link", { name: "pivotable-unsafe_fakeuser" }).click();
	await page.getByRole("button", { name: "Login fakeUser" }).click();

	await page.getByRole("link", { name: "Browse through endpoints" }).click();
	await page
		.getByRole("link", { name: /simple/i })
		.first()
		.click();
	await page.getByRole("link", { name: /Query simple/i }).click();

	// ── Build a working query: city + delta ──
	await queryPivotable.addColumn(page, "city");
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("delta");
	await page.getByRole("switch", { name: "JSON" }).uncheck();
	await page.getByRole("button", { name: /measures/ }).click();
	await page.getByRole("switch", { name: /^delta$/ }).check();

	await expect(page.locator(".slick-row").first()).toBeVisible();

	// Snapshot the switches that should survive the restore.
	await expect(page.getByRole("switch", { name: /^delta$/ })).toBeChecked();

	// ── Break the query by adding the always-throwing measure ──
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("always_throws");
	await page.getByRole("switch", { name: /^always_throws$/ }).check();

	// Banner appears.
	const errorBanner = page.getByText(/Query is broken/);
	await expect(errorBanner).toBeVisible();

	// Wizard confirms the faulty measure is selected (repro of the pre-fix state).
	await expect(page.getByRole("switch", { name: /^always_throws$/ })).toBeChecked();

	// ── Click "Restore last successful query" ──
	await page.getByRole("button", { name: /Restore last successful query/ }).click();

	// EXPECTED: banner hidden.
	await expect(errorBanner).toBeHidden();

	// EXPECTED: the wizard reflects the restored state — `always_throws` unchecked.
	// This is the specific check requested: pre-fix, the switch stayed visually checked
	// even though selectedMeasures.always_throws had been logically dropped.
	await expect(page.getByRole("switch", { name: /^always_throws$/ })).not.toBeChecked();

	// And the surviving measure stays selected.
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("delta");
	await expect(page.getByRole("switch", { name: /^delta$/ })).toBeChecked();

	// Grid still shows data (the last successful view is preserved).
	await expect(page.locator(".slick-row").first()).toBeVisible();
});
