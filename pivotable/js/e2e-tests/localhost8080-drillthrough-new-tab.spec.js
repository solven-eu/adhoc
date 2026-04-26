import { test, expect } from "./_coverage-fixture.mjs";

import queryPivotable from "./query-pivotable.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// Nominal end-to-end scenario for the cell-modal "DrillThrough in new tab" affordance.
//
// Flow: an aggregated cube query is built; the user double-clicks a row, the cell modal opens, and the
// "DrillThrough in new tab" button is clicked. Expectation:
//   1. A new browser tab opens whose URL ends with `#<encoded JSON>` of a queryModel that
//      - contains `DRILLTHROUGH` in its options, and
//      - includes a `column-equals` filter for the clicked row's groupBy column (`country`).
//   2. The originating tab's URL hash is left untouched (the new-tab path must NOT mutate the current
//      queryModel).
//
// This pins the contract end-to-end: the URL hash is the only thing the new tab sees, so the encoded
// queryModel JSON is the right place to assert. We don't wait for the new tab's Vue app to render —
// hash inspection is sufficient and far more stable than DOM-shape assertions on a freshly-loaded SPA.
test.setTimeout(60_000);
test("DrillThrough in new tab: opens fresh tab with DT option pinned in URL hash", async ({ page, context }) => {
	const isViewResponse = (resp) => {
		const u = resp.url();
		return u.includes("/api/v1/cubes/query/result") && u.includes("with_view=true") && resp.ok();
	};

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

	// Build an aggregated query: groupBy country + measure delta.
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("country");
	await page.getByRole("button", { name: /columns/ }).click();
	await page.locator('[id="column_country"]').check();

	const aggregatedResponsePromise = page.waitForResponse(isViewResponse, { timeout: 30_000 });
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("delta");
	await page.getByRole("switch", { name: "JSON" }).uncheck();
	await page.getByRole("button", { name: /measures/ }).click();
	await page.locator('[id="measure_delta"]').check();
	await aggregatedResponsePromise;

	// Wait for the grid to render.
	await expect(page.locator(".slick-row").first()).toBeVisible();

	// Snapshot the originating tab's URL hash so we can verify it's untouched at the end.
	const originalUrl = page.url();

	// Double-click the first data row to open the cell modal.
	await page.locator(".slick-row").first().dblclick();
	await expect(page.locator("#cellModal")).toBeVisible();

	// Click "DrillThrough in new tab" — captures the new tab via context.waitForEvent("page").
	const newTabPromise = context.waitForEvent("page", { timeout: 15_000 });
	await page.getByRole("button", { name: /DrillThrough in new tab/ }).click();
	const newTab = await newTabPromise;

	// The new tab's URL must encode the queryModel in its hash; we don't depend on the new tab's
	// Vue app rendering — read the URL directly. The hash is encodeURIComponent(JSON.stringify(...)),
	// so decode-and-parse to assert structure.
	const newTabUrl = newTab.url();
	expect(newTabUrl).toContain("/cubes/simple/query");
	expect(newTabUrl).toContain("#");

	const hashEncoded = newTabUrl.split("#", 2)[1];
	expect(hashEncoded).toBeTruthy();
	const hashDecoded = decodeURIComponent(hashEncoded);
	const parsed = JSON.parse(hashDecoded);

	// DRILLTHROUGH is now in the options array.
	expect(parsed.query.options).toContain("DRILLTHROUGH");

	// A `column=country` equals filter was appended (cell coordinate pinned).
	const filters = (parsed.query.filter && parsed.query.filter.filters) || [];
	const countryFilter = filters.find((f) => f.type === "column" && f.column === "country");
	expect(countryFilter).toBeTruthy();
	expect(countryFilter.valueMatcher).toBeTruthy();

	// The originating tab must NOT have been mutated — its URL hash is unchanged.
	expect(page.url()).toBe(originalUrl);

	await newTab.close();
});
