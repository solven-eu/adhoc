// @ts-check
import { test, expect } from "./_coverage-fixture.mjs";

import queryPivotable from "./query-pivotable.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// Exercises the missing-value placeholders the SlickGrid DT formatters render in DRILLTHROUGH mode. The
// formatter picks between two flavours:
//
//   - `NULL`      : when the column IS in the user's query (groupBy or measures) and the source row carries
//                   a null/missing value for it. The user asked for the column — the placeholder reads as
//                   "the data is null", not "the column doesn't apply".
//   - `Out of DT` : when the column is NOT in the user's query but appeared in the DT response anyway (e.g.
//                   because the engine surfaced an underlying aggregator from a Combinator, or because the
//                   row-inclusion filter added a column to the groupBy). The user did not explicitly request
//                   the column, so a missing value means it falls outside the DT contract for this row.
//
// This test exercises the `NULL` flavour by querying `theta` on the `simple` cube. `theta` IS in the user-
// submitted measures, but the seed only fills it on ~50% of rows — rows lacking `theta` therefore produce DT
// entries whose `values` map has no `theta` key, and the grid must render `NULL` (NOT `Out of DT`) in those
// cells.
test.setTimeout(60_000);
test("DRILLTHROUGH grid renders `NULL` placeholder for requested column missing on source row", async ({ page }) => {
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

	// Build the query: groupBy `country`, measures `delta` + `theta`. Each step waits for the resulting
	// `with_view=true` response so the wizard's debounced query state has settled before the next click.
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("country");
	await page.getByRole("button", { name: /columns/ }).click();
	await page.locator('[id="column_country"]').check();

	// Single measure: `theta`. The sparse data column declared in `InjectSimpleExampleCubesConfig` is filled
	// only on ~50% of source rows. In DT, the rows without `theta` produce entries whose `values` map lacks
	// the `theta` key — `theta` IS in the user's query so the formatter must render the placeholder as
	// `NULL`, not `Out of DT`.
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("theta");
	await page.getByRole("switch", { name: "JSON" }).uncheck();
	await page.getByRole("button", { name: /measures/ }).click();
	const thetaCheckbox = page.locator("input[id='measure_theta']");
	await expect(thetaCheckbox).toBeVisible({ timeout: 10_000 });
	const responsePromise = page.waitForResponse(isViewResponse, { timeout: 30_000 });
	// `force: true` — the Vue reactive re-renders after the search-box `fill` keep the checkbox attached/
	// detached for a few moments. Playwright's default stability check times out before the element settles,
	// even though the eventual state is fine. Forcing the click sidesteps the heuristic; the subsequent
	// `toBeChecked()` confirms the click actually toggled the input.
	await thetaCheckbox.check({ force: true });
	await responsePromise;
	await expect(thetaCheckbox).toBeChecked();

	await expect(page.locator(".slick-row").first()).toBeVisible();

	// Toggle DRILLTHROUGH on.
	await page.getByRole("button", { name: /^\d+ options/ }).click();
	const drillthroughResponsePromise = page.waitForResponse(isViewResponse, { timeout: 30_000 });
	await page.locator('[id="option_DRILLTHROUGH"]').check();
	const drillthroughResponse = await drillthroughResponsePromise;
	const drillthroughView = (await drillthroughResponse.json()).view;

	// Sanity: the DT response carries at least one row whose `values` map lacks `theta` (rows where the
	// source did not have the column), AND at least one row that DOES carry it — heterogeneity, otherwise the
	// test setup is broken and the placeholder has nothing to render.
	const rowsWithTheta = drillthroughView.values.filter((v) => "theta" in v && v.theta != null);
	const rowsMissingTheta = drillthroughView.values.filter((v) => !("theta" in v) || v.theta == null);
	expect(rowsWithTheta.length).toBeGreaterThan(0);
	expect(rowsMissingTheta.length).toBeGreaterThan(0);

	// The grid must have rendered at least one cell carrying the `NULL` placeholder (theta IS in the user
	// query, so missing values must NOT use the `Out of DT` placeholder).
	const nullCells = page.locator('.slick-cell:has-text("NULL")');
	await expect(nullCells.first()).toBeVisible({ timeout: 10_000 });

	// And the wrong placeholder must NOT appear: `theta` is part of the user-submitted query, so it cannot
	// be classified as "Out of DT". This guards the requestedColumns wiring from regressing back to a
	// catch-all placeholder.
	const outOfDtCells = page.locator('.slick-cell:has-text("Out of DT")');
	await expect(outOfDtCells).toHaveCount(0);

	// The placeholder must carry the grey-italic styling — otherwise the user can't distinguish it from any
	// other empty-looking content.
	const placeholderSpan = page.locator('.slick-cell span:has-text("NULL")').first();
	await expect(placeholderSpan).toHaveCSS("font-style", "italic");
});
