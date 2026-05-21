// @ts-check
import { test, expect, request } from "@playwright/test";

import queryPivotable from "./query-pivotable.mjs";

import { BASE_URL as url } from "./_url.mjs";

test.beforeAll(async ({ request }) => {
	// Create a new repository
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// We just check the login page is working OK
test("showLoginOptions", async ({ page }) => {
	await page.goto(url);
	await queryPivotable.showLoginOptions(page);
});

test("login", async ({ page }) => {
	await page.goto(url);
	await queryPivotable.login(page);
});

test("queryPivotable", async ({ page }) => {
	await page.goto(url);
	await queryPivotable.queryPivotable(page);
});

// We observed some reactivity issues on adding columns
test("queryPivotable.addColumn", async ({ page }) => {
	await page.goto(url);
	await queryPivotable.queryPivotable(page);

	await queryPivotable.addColumn(page, "Shirt Number");

	// TODO Wait for query executed
	// row0. event_count is integer-only → the cell formatter drops decimals (see
	// `formatNumber` + `stats.allInteger` in adhoc-query-grid-helper.js), so the rendered
	// value is "28" rather than "28.00".
	await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(0)).toHaveText("0");
	await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(1)).toHaveText("C");
	await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(2)).toHaveText("0");
	await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(3)).toHaveText("28");

	// row1
	await expect(page.locator(".slick-row").nth(1).locator(".slick-cell").nth(0)).toHaveText("1");
	// rowSpan leads to `Position=C` on second row not to exist
	//		await expect(page.locator(".slick-row").nth(1).locator(".slick-cell").nth(1)).toHaveText("C");
	await expect(
		page
			.locator(".slick-row")
			.nth(1)
			.locator(".slick-cell")
			.nth(2 - 1),
	).toHaveText("2");
	await expect(
		page
			.locator(".slick-row")
			.nth(1)
			.locator(".slick-cell")
			.nth(3 - 1),
	).toHaveText("17");
});

test("queryPivotable.addDependantFromGraph", async ({ page }) => {
	await page.goto(url);
	await queryPivotable.queryPivotable(page);

	// Hover the column header to surface the SlickHeaderButtons icon — the per-icon trio was
	// replaced by a single 3-dot menu (tooltip "Measure actions") to keep destructive actions
	// away from the column-resize handle. Two clicks now: open the menu, then pick "Show DAG".
	await page
		.getByRole("columnheader", { name: /event_count/ })
		.first()
		.hover();
	await page.getByTitle("Measure actions").first().click();
	await page
		.locator(".adhoc-column-menu")
		.getByText(/Show DAG/)
		.click();
	await page.locator("a").filter({ hasText: "goal_count" }).click();
	await page.getByRole("dialog", { name: "Measure Info" }).getByLabel("Close").click();

	// event_count / goal_count are integer-only columns — the formatter drops fractional
	// digits (see `formatNumber` + `stats.allInteger` in adhoc-query-grid-helper.js), so the
	// rendered values omit ".00" that the previous assertions expected.
	const starCoordinate = false;
	if (starCoordinate) {
		// row0
		await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(2)).toHaveText("11,270");
		await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(3)).toHaveText("2,256");
		// row1
		await expect(page.locator(".slick-row").nth(1).locator(".slick-cell").nth(2)).toHaveText("558");
		await expect(page.locator(".slick-row").nth(1).locator(".slick-cell").nth(3)).toHaveText("158");
	} else {
		// row0
		await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(2)).toHaveText("558");
		await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(3)).toHaveText("158");
		// row1 — cell[3] holds a null value that is now rendered with the explicit `NULL`
		// placeholder (per CHANGES.MD: "empty cells are now rendered with explicit greyed-italic
		// placeholders"). The cell still has `title="The cell carries a null value."` so we anchor
		// on that attribute instead of the empty-text check the renderer no longer produces.
		await expect(page.locator(".slick-row").nth(1).locator(".slick-cell").nth(2)).toHaveText("88");
		await expect(page.locator(".slick-row").nth(1).locator(".slick-cell").nth(3)).toHaveAttribute("title", "The cell carries a null value.");
	}
});

test("queryPivotable.browserRefresh", async ({ page }) => {
	await page.goto(url);
	await queryPivotable.queryPivotable(page);

	await page.reload();

	// Check the pivotTable data is available
	const starCoordinate = false;
	if (starCoordinate) {
		await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(2)).toHaveText("11,270");
	} else {
		await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(2)).toHaveText("558");
	}
});
