import { test, expect } from "./_coverage-fixture.mjs";

import queryPivotable from "./query-pivotable.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// Exercises the "Out of DT" placeholder added to the SlickGrid formatters: in DRILLTHROUGH mode, a cell whose
// value is null/undefined (because the source row does not carry that column) renders as `Out of DT` in grey
// italic instead of as an empty cell.
//
// Strategy: stub the `with_view=true` GET that delivers the DT result with a synthetic heterogeneous payload.
// Driving the engine end-to-end to produce a heterogeneous response is brittle (the merger's per-aggregator
// FILTER handling depends on cube-side rewrites that are not directly exposed through the wizard). Intercepting
// the response lets us isolate the test to the formatter contract: given a DT view where some rows lack a key
// in `values`, the grid renders the placeholder for those cells.
test.setTimeout(60_000);
test("DRILLTHROUGH grid renders `Out of DT` placeholder for cells missing in heterogeneous response", async ({ page }) => {
	const isViewResultUrl = (u) => u.includes("/api/v1/cubes/query/result") && u.includes("with_view=true");

	// Stub every `with_view=true` GET (sync polling of the async query result endpoint) with a heterogeneous
	// DT payload. Three rows: two have `delta_filtered` populated, one does NOT — the grid must render the
	// missing cell as `Out of DT`.
	const stubbedView = {
		query: {
			filter: "matchAll",
			groupBy: { columns: ["country"] },
			measures: ["delta", "delta_filtered"],
			options: ["DRILLTHROUGH"],
		},
		coordinates: [{ country: "France" }, { country: "Spain" }, { country: "France" }],
		values: [{ delta: 1.5, delta_filtered: 1.5 }, { delta: 2.5 /* no `delta_filtered` for non-France */ }, { delta: 3.5, delta_filtered: 3.5 }],
	};
	await page.route(
		(url) => isViewResultUrl(url.toString()),
		async (route) => {
			await route.fulfill({
				contentType: "application/json",
				body: JSON.stringify({ view: stubbedView }),
			});
		},
	);

	const isViewResponse = (resp) => isViewResultUrl(resp.url()) && resp.ok();

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

	// Build a minimal query: column `country` + measure `delta`. The exact selection is mostly irrelevant —
	// the stubbed view above is what the grid will render — but we still need a real submitted query to
	// trigger the polling that hits the intercepted endpoint.
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

	// Toggle DRILLTHROUGH on so the grid uses the DT rendering path (heterogeneous-schema discovery + DT
	// formatter). The stubbed response is returned regardless of the query state, but DT mode is what wires
	// in the `Out of DT` formatter.
	await page.getByRole("button", { name: /^\d+ options/ }).click();
	const drillthroughResponsePromise = page.waitForResponse(isViewResponse, { timeout: 30_000 });
	await page.locator('[id="option_DRILLTHROUGH"]').check();
	await drillthroughResponsePromise;

	// The grid must render at least one cell carrying the `Out of DT` placeholder. SlickGrid emits the
	// rendered DOM into `.slick-cell` nodes; the DT formatter returns the placeholder via `html`, so the
	// text lands inside a `<span style="color:#888;font-style:italic">Out of DT</span>` nested in a cell.
	const placeholderCells = page.locator('.slick-cell:has-text("Out of DT")');
	await expect(placeholderCells.first()).toBeVisible({ timeout: 10_000 });

	// The placeholder must carry the grey-italic styling — otherwise the user can't distinguish it from any
	// other empty-looking content.
	const placeholderSpan = page.locator('.slick-cell span:has-text("Out of DT")').first();
	await expect(placeholderSpan).toHaveCSS("font-style", "italic");
});
