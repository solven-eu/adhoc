import { test, expect } from "./_coverage-fixture.mjs";

import queryPivotable from "./query-pivotable.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// Regression test for the singleton grid-header column-filter modal bug.
//
// Repro reported by user: on the `simple` cube, build a query with two columns (city +
// country) and a measure, then in the rendered grid click the filter icon in the `city`
// column header, pick the `equals` operator, type a value and confirm. Now click the
// filter icon in the `country` column header — the freetext input is pre-filled with the
// value we just typed for `city`. Expected: the input should be empty (or, if `country`
// already has a saved filter, preloaded with that column's own value).
//
// Root cause: `adhoc-query-wizard-column-filter-modal-singleton.js` mounts ONE child
// `AdhocQueryWizardColumnFilter` component and changes its `:column` prop across clicks
// — `setup()` only runs once, so internal `ref("")` state (`equalsValue`, `filterType`…)
// carries across columns. Fix: watch `props.column` in the child and reset local state
// (and preload from the already-saved filter for that column) on every change.
test("Grid-header column filter modal resets state when switching columns", async ({ page }) => {
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

	// Add two columns. Target the switches by their stable `id` attribute (the wizard
	// emits `column_<name>` / `measure_<name>` ids) so loose-match collisions with
	// other entries (e.g. `city` vs `capital_city`) cannot occur.
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("city");
	await page.getByRole("button", { name: /columns/ }).click();
	await page.locator('[id="column_city"]').check();

	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("country");
	await page.locator('[id="column_country"]').check();

	// A measure is required to get any grid rows — the grid is how we reach the header
	// filter icons (SlickHeaderButtons plugin, rendered only when rows exist).
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("delta");
	await page.getByRole("switch", { name: "JSON" }).uncheck();
	await page.getByRole("button", { name: /measures/ }).click();
	await page.locator('[id="measure_delta"]').check();

	await expect(page.locator(".slick-row").first()).toBeVisible();

	// Helper: click the `.bi-filter-circle` button under the SlickGrid header matching
	// `columnName`. SlickGrid renders each column header as `.slick-header-column` with
	// its `.slick-column-name` text, and `SlickHeaderButtons` injects the filter icon as
	// a descendant `.slick-header-button.bi-filter-circle`. The column name is now
	// wrapped in `<span class="adhoc-header-name">{name}</span>` followed by the inline
	// copy-name icon, so we match the inner span exactly rather than the whole
	// `.slick-column-name` text (which would include trailing whitespace).
	const openHeaderFilter = async (columnName) => {
		const header = page.locator(".slick-header-column").filter({
			has: page.locator(".adhoc-header-name", { hasText: new RegExp(`^${columnName}$`) }),
		});
		await header.hover();
		await header.locator(".slick-header-button.bi-filter-circle").click({ force: true });
		// The singleton modal is always #columnFilterModal (no column suffix).
		await expect(page.locator("#columnFilterModal")).toBeVisible();
	};

	// Step 1 — open the filter modal on `city` via the grid header and type a value into
	// the equals input. We intentionally DO NOT save (Ok), because saving would apply the
	// filter to the grid, and an exotic value like "e2e-city-value" would reduce the
	// result to zero rows — SlickGrid then drops the data columns and we lose the header
	// filter icon on `country`. Dismissing without saving is enough to exhibit the bug:
	// the singleton modal's child component retains `equalsValue` in memory across the
	// subsequent `:column` prop change.
	await openHeaderFilter("city");
	await page.locator("#columnFilterModal").getByLabel("Filter type").selectOption("equals");
	await page.locator("#columnFilterModal").getByLabel("Filter value").fill("e2e-city-value");
	await page.locator("#columnFilterModal").getByRole("button", { name: "Close" }).click();
	await expect(page.locator("#columnFilterModal")).toBeHidden();

	// Step 2 — open the filter modal for `country`. Pre-fix, the `equalsValue` ref from
	// the `city` modal would still be attached to the (singleton) child component, so
	// selecting `equals` on `country` would show "e2e-city-value" in the textbox.
	await openHeaderFilter("country");
	await page.locator("#columnFilterModal").getByLabel("Filter type").selectOption("equals");

	// The filter-value input for `country` must start EMPTY — it must not leak `city`'s
	// typed value. This is the regression assertion.
	await expect(page.locator("#columnFilterModal").getByLabel("Filter value")).toHaveValue("");
});
