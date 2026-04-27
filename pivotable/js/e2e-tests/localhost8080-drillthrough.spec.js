import { test, expect } from "./_coverage-fixture.mjs";

import queryPivotable from "./query-pivotable.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// Nominal end-to-end scenario for the DRILLTHROUGH option on the `simple` cube.
//
// Why this lives at the e2e layer (and not as a vitest unit test): DRILLTHROUGH is a
// transverse, high-level feature that crosses the wire (CubeQueryEngine.executeDrillthrough
// → ListBasedTabularView JSON → grid render). The set of components on the path is large
// (wizard option toggle, queryJson watcher, /api/v1/cubes/query controller, ListBasedTabularView
// adaptor, slick grid resync) and we want the contract to survive any internal refactor of
// those components. Asserting on observable browser+network behaviour pins the contract;
// asserting on internal data structures would couple the test to today's component shape
// and silently rot.
//
// What we assert:
//   1. The non-DRILLTHROUGH query against the `simple` cube returns one row per `country`
//      slice (the cube has ~200 distinct countries from the Faker sample data).
//   2. Toggling DRILLTHROUGH in the wizard's options accordion fires a new query whose
//      response carries one entry per source row of the table — i.e. far more rows than
//      the country count.
//   3. The DRILLTHROUGH response keys the per-aggregator value column under the alias
//      `delta` (no FILTER conflict, so the alias collapses to the aggregator name).
//
// Each query goes through the asynchronous endpoint
// (POST /api/v1/cubes/query/asynchronous → poll /api/v1/cubes/query/result?…with_view=true);
// we wait on the final `with_view=true` GET that carries the actual ITabularView payload.
test.setTimeout(60_000);
test("DRILLTHROUGH on simple cube: option toggle returns one row per source row", async ({ page }) => {
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

	// Build a minimal aggregated query: one column (`country`) + one measure (`delta`). The
	// row count returned by this query is the number of distinct countries in the sample.
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

	const aggregatedResponse = await aggregatedResponsePromise;
	const aggregatedView = (await aggregatedResponse.json()).view;

	// Sanity: the aggregated response is the parallel-arrays form (`coordinates` / `values`)
	// produced by `ListBasedTabularView`. Row count is bounded by the number of distinct
	// countries; pick a generous upper bound that's still well below the source-row count.
	expect(Array.isArray(aggregatedView.coordinates)).toBeTruthy();
	expect(Array.isArray(aggregatedView.values)).toBeTruthy();
	expect(aggregatedView.coordinates.length).toBe(aggregatedView.values.length);
	const aggregatedRowCount = aggregatedView.coordinates.length;
	expect(aggregatedRowCount).toBeGreaterThan(0);
	expect(aggregatedRowCount).toBeLessThan(1000);

	// Wait for the grid to settle on the aggregated view before flipping the option.
	await expect(page.locator(".slick-row").first()).toBeVisible();

	// Open the Options accordion. The accordion header button's accessible name is
	// "<N> options <S>" where N is the available count and S the currently-selected count
	// (both rendered as adjacent badges) — match by `options` substring with anchored leading
	// digit to avoid false positives on other "options"-themed labels.
	await page.getByRole("button", { name: /^\d+ options/ }).click();

	// Toggle DRILLTHROUGH on. Each option checkbox carries a stable id `option_<NAME>`
	// (see `adhoc-query-wizard-options.js`), independent of label wording.
	const drillthroughResponsePromise = page.waitForResponse(isViewResponse, { timeout: 30_000 });
	await page.locator('[id="option_DRILLTHROUGH"]').check();
	const drillthroughResponse = await drillthroughResponsePromise;
	const drillthroughView = (await drillthroughResponse.json()).view;

	// DRILLTHROUGH returns one entry per source row of the table — the `simple` cube's
	// in-memory table is seeded with 16 * 1024 rows (`InjectSimpleExampleCubesConfig`), so
	// the response must carry far more entries than the aggregated country count.
	expect(Array.isArray(drillthroughView.coordinates)).toBeTruthy();
	expect(Array.isArray(drillthroughView.values)).toBeTruthy();
	expect(drillthroughView.coordinates.length).toBe(drillthroughView.values.length);
	expect(drillthroughView.coordinates.length).toBeGreaterThan(aggregatedRowCount * 10);

	// Spot-check a row's shape: `coordinates` carries the merged groupBy (`country`) and
	// `values` carries the per-aggregator alias (`delta`). With a single, unfiltered
	// aggregator the alias collapses to its name (no `_<index>` suffix).
	const firstValues = drillthroughView.values[0];
	expect(Object.keys(firstValues)).toContain("delta");

	// And the grid still renders without crashing — the heterogeneous-schema handling and
	// the `sanityCheckFirstRow` gate must hold for the DRILLTHROUGH branch.
	await expect(page.locator(".slick-row").first()).toBeVisible();
});
