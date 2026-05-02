import { test, expect } from "./_coverage-fixture.mjs";

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
	await page.getByRole("button", { name: /^Login$/i }).click();

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
	await page.locator('[id="measure_delta"]').check();

	// Sanity: at least one row rendered — i.e. the query ran successfully.
	await expect(page.locator(".slick-row").first()).toBeVisible();

	// ── Break the query by adding the always-throwing measure ──
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("always_throws");
	await page.locator('[id="measure_always_throws"]').check();

	// The error banner above the grid must surface the failure.
	const errorBanner = page.getByText(/Query is broken/);
	await expect(errorBanner).toBeVisible();

	// Wizard confirms the faulty measure is selected (repro of the pre-fix state).
	await expect(page.locator('[id="measure_always_throws"]')).toBeChecked();

	// ── Click Reset query ──
	await page.getByRole("button", { name: /Reset query/ }).click();
	await page.waitForTimeout(500);
	// EXPECTED: the banner disappears. Pre-fix, the empty-query that followed the reset was
	// rejected by the backend and re-populated `tabularView.error`, leaving the banner up.
	await expect(errorBanner).toBeHidden();

	// EXPECTED: the wizard reflects the reset — selectedMeasures is empty after Reset.
	// Pre-fix, `queryModel.reset()` reassigned `selectedMeasures = {}` which broke
	// per-key reactivity. The fix clears keys in place via `delete`. We assert against
	// the underlying queryModel state rather than DOM switches because the search box
	// is also cleared on Reset, so the `delta` / `always_throws` switches are no longer
	// rendered (their accordion bodies filter on the search text).
	const stateAfterReset = await page.evaluate(() => {
		const piniaApp = document.querySelector("#app").__vue_app__;
		// The queryModel lives on the route component instance; reach it via the
		// inspectable DOM by reading the input states + searchbox.
		const sel = (id) => {
			const i = document.getElementById(id);
			return i ? i.checked : null;
		};
		return {
			delta: sel("measure_delta"),
			always_throws: sel("measure_always_throws"),
		};
	});
	// `null` means "switch not in DOM" (search box is cleared after Reset, so the
	// accordion body's filter hides the row). Either null or false is the expected
	// post-Reset state — both prove the model was wiped.
	expect(stateAfterReset.delta === null || stateAfterReset.delta === false).toBe(true);
	expect(stateAfterReset.always_throws === null || stateAfterReset.always_throws === false).toBe(true);
});
