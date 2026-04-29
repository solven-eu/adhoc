import { test, expect } from "./_coverage-fixture.mjs";

import queryPivotable from "./query-pivotable.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// Nominal end-to-end scenario for the AdhocColumnChip menu rendered inside filter pills.
//
// Flow: build a query with a `country=FR` filter, open the chip's dropdown on the `country`
// column ref inside the wizard's filter pill, click "Add as groupBy", and assert that the column
// appears in the groupBy state (selected as a column).
//
// The chip's "Add filter" / "Edit filter" actions both open the existing column-filter modal — they
// are not exercised here to avoid coupling to the modal's internal markup; this spec pins the
// most user-visible action ("add as groupBy") which is the chip's primary value-add over an inert
// column-name span.
test.setTimeout(60_000);
test("Column chip: 'Add as groupBy' from a filter pill toggles the column into selectedColumns", async ({ page }) => {
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

	// Add a `country=FR` filter via the search wizard. We rely on the existing filter modal flow:
	// open the columns search, find country, click its filter button, type a value, save.
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("country");
	await page.getByRole("button", { name: /columns/ }).click();
	// The column-filter modal singleton is mounted lazily — open the modal then add a value.
	const filterButton = page.locator('[title="Edit filter on country"]').first();
	await filterButton.click();
	// Confirm the modal opened — Bootstrap modals get the `show` class.
	await expect(page.locator("#columnFilterModal_country")).toHaveClass(/show/);
	// The modal opens with `filterType="no_filter"` (only a select is rendered). The text input
	// for `equals` value only appears once we pick that filter type — flip the select first.
	await page.locator("#columnFilterModal_country select").first().selectOption("equals");
	// Use the modal's free-text matcher to add a value.
	await page.locator("#columnFilterModal_country input[type=text]").first().fill("FR");
	// Save — the modal's save button text varies; pick by `type=submit` or its label.
	await page
		.locator("#columnFilterModal_country")
		.getByRole("button", { name: /save|ok|apply/i })
		.click();

	// Wait for the chip to render in the filter sidebar — the chip is a clickable <a> with the column name.
	const countryChip = page.locator(".dropdown a", { hasText: "country" }).first();
	await expect(countryChip).toBeVisible();

	// Open the chip dropdown and click "Add as groupBy".
	await countryChip.click();
	const groupByResponsePromise = page.waitForResponse(isViewResponse, { timeout: 30_000 });
	await page.getByRole("button", { name: /Add as groupBy/ }).click();
	await groupByResponsePromise;

	// The country column toggle in the wizard's columns accordion must now be checked.
	await page.getByRole("button", { name: /columns/ }).click();
	await expect(page.locator('[id="column_country"]')).toBeChecked();
});
