import { test, expect } from "./_coverage-fixture.mjs";

import queryPivotable from "./query-pivotable.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// Exercises the grid's `scroll` layout mode (toggled from the grid controls). Verifies:
//   1. The toggle button is present and reflects the current layout.
//   2. Switching to `scroll` mode adds horizontal scrolling capability to the grid viewport.
//   3. A phantom trailing column is appended (visually invisible) so the user can grab and resize
//      the LAST real column's right border — the whole reason this mode exists.
//   4. Switching back to `fit` removes the phantom column and restores the no-scroll layout.
test.setTimeout(60_000);
test("Grid layout toggle: fit ↔ scroll, phantom column appears only in scroll mode", async ({ page }) => {
	const isViewResponse = (resp) => {
		const u = resp.url();
		return u.includes("/api/v1/cubes/query/result") && u.includes("with_view=true") && resp.ok();
	};

	// Reset gridLayout to `fit` from any prior test run (preferences persist in localStorage and
	// `_coverage-fixture.mjs` does not clear it). Without this, a previous `scroll`-mode run would
	// leave the toggle in the wrong starting state.
	await page.addInitScript(() => {
		try {
			const raw = localStorage.getItem("adhoc.preferences");
			if (raw) {
				const parsed = JSON.parse(raw);
				parsed.gridLayout = "fit";
				localStorage.setItem("adhoc.preferences", JSON.stringify(parsed));
			}
		} catch (e) {
			// ignore — fresh browser context will hydrate with the default `fit`
		}
	});

	await page.goto(url);
	await page.getByRole("link", { name: /You need to login/ }).click();
	await page.getByRole("link", { name: "pivotable-unsafe_fakeuser" }).click();
	await page.getByRole("button", { name: /^Login$/i }).click();

	await page.getByRole("link", { name: "Browse through endpoints" }).click();
	await page
		.getByRole("link", { name: /simple/i })
		.first()
		.click();
	await page.getByRole("link", { name: /Query simple/i }).click();

	// Build a minimal query: groupBy `country` + measure `delta`. Enough to populate the grid so
	// the toggle has something to act on.
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("country");
	await page.getByRole("button", { name: /columns/ }).click();
	await page.locator('[id="column_country"]').check();

	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("delta");
	await page.getByRole("switch", { name: "JSON" }).uncheck();
	await page.getByRole("button", { name: /measures/ }).click();
	const aggregatedResponsePromise = page.waitForResponse(isViewResponse, { timeout: 30_000 });
	await page.locator('[id="measure_delta"]').check();
	await aggregatedResponsePromise;

	await expect(page.locator(".slick-row").first()).toBeVisible();

	// In `fit` mode the toggle button shows "Scroll" (the action it would perform).
	const toggle = page.getByRole("button", { name: "Scroll" });
	await expect(toggle).toBeVisible();

	// In `fit` mode there is no phantom column.
	await expect(page.locator(".slick-header-phantom")).toHaveCount(0);

	// Click the toggle. The grid resyncs into `scroll` mode — the button label flips to "Fit width",
	// and the phantom trailing column appears in the DOM.
	await toggle.click();
	await expect(page.getByRole("button", { name: "Fit width" })).toBeVisible();
	await expect(page.locator(".slick-header-phantom").first()).toBeVisible({ timeout: 5_000 });

	// The phantom column must have non-zero width (the headroom for resizing the last real column).
	// We assert via getBoundingClientRect rather than CSS `width` because SlickGrid sets the width
	// via an inline pixel value.
	const phantomWidth = await page
		.locator(".slick-header-phantom")
		.first()
		.evaluate((el) => el.getBoundingClientRect().width);
	expect(phantomWidth).toBeGreaterThan(50);

	// Toggle back: `Scroll` label returns and the phantom column disappears.
	await page.getByRole("button", { name: "Fit width" }).click();
	await expect(page.getByRole("button", { name: "Scroll" })).toBeVisible();
	await expect(page.locator(".slick-header-phantom")).toHaveCount(0);
});
