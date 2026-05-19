// @ts-check
import { expect } from "@playwright/test";

const addColumn = async function (page, column) {
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill(column);
	await page.getByRole("button", { name: /columns/ }).click();
	// Target by stable id (`column_<name>`). The accessible name of the column
	// switch is matched substring-wise by Playwright, so passing `"city"` would
	// also match `capital_city`. Same trade-off as `addMeasure`.
	await page.locator(`[id="column_${column}"]`).check();
};

const addMeasure = async function (page, measure) {
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill(measure);
	// Disable JSON to prevent the depending measures to show up
	await page.getByRole("switch", { name: "JSON" }).uncheck();

	// Target by stable id (`measure_<name>`). The accessible name of the switch now
	// embeds the measure description / dependency hints, so a name-based match on
	// `event_count` would also match dependant measures whose description references
	// it — Playwright then trips on strict mode.
	await page.getByRole("button", { name: /measures/ }).click();
	await page.locator(`[id="measure_${measure}"]`).check();
};

// https://stackoverflow.com/questions/33178843/es6-export-default-with-multiple-functions-referring-to-each-other
export default {
	async clear(request, url) {
		const response = await request.post(url + "/api/v1/clear");

		return response;
	},

	async showLoginOptions(page) {
		await page.getByRole("link", { name: /You need to login/ }).click();

		await expect(page.getByRole("link", { name: /github/ })).toBeVisible();
		await expect(page.getByRole("link", { name: /pivotable-unsafe_fakeuser/ })).toBeVisible();
	},

	async login(page) {
		await this.showLoginOptions(page);
		await page.getByRole("link", { name: /pivotable-unsafe_fakeuser/ }).click();
		await page.getByRole("button", { name: /^Login$/i }).click();

		// The login chip now opens an in-SPA Bootstrap modal instead of routing to /html/login,
		// so the post-login user lands back on whatever page hosted the chip (typically `/`).
		// The previous "Welcome Fake User" assertion targeted the /html/login card; the new
		// stable signal across every host page is the navbar's "Logged in as <name>" entry.
		await expect(page.getByText(/Logged in as Fake User/)).toBeVisible();
	},

	addColumn: addColumn,

	async queryPivotable(page) {
		await page.goto("http://localhost:8080/");
		await page.getByRole("link", { name: /You need to login/ }).click();
		await page.getByRole("link", { name: "pivotable-unsafe_fakeuser" }).click();
		await page.getByRole("button", { name: /^Login$/i }).click();
		await page.getByRole("link", { name: "Browse through endpoints" }).click();
		await page.getByRole("link", { name: /WorldCupPlayers/ }).click();
		await page.getByRole("link", { name: /Query WorldCupPlayers/ }).click();

		await addColumn(page, "Position");

		await addMeasure(page, "event_count");

		const starCoordinate = false;
		if (starCoordinate) {
			// Check some measure value. event_count is an all-integer column — the cell
			// formatter detects that and drops fractional digits, so the rendered value is
			// "11,270" (not "11,270.00"). See `computeMeasureStats.allInteger` +
			// `formatNumber`'s integer-format override in adhoc-query-grid-helper.js.
			await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(1)).toContainText("*");
			await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(2)).toContainText("11,270");
		} else {
			await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(1)).toContainText("C");
			await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(2)).toContainText("558");
		}
	},
};
