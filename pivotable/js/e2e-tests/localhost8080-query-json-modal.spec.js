import { test, expect } from "./_coverage-fixture.mjs";

import queryPivotable from "./query-pivotable.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// E2E coverage for the wizard's "JSON" modal (AdhocQueryRawModal).
//
// Regression target: the modal lives inside the floating Submit block, which uses
// `transform: translate(...)` for centering. A `transform` creates a containing block
// for fixed-position descendants — without `<Teleport to="body">`, the modal-backdrop
// covers the modal itself and the tab strip is positioned off-screen (the user sees a
// fully greyed-out viewport with no JSON option visible). The modal must teleport to
// <body> so it lays out against the viewport as Bootstrap expects.
//
// We exercise the four tabs (JSON / MDX / SQL / Mermaid) to make sure each renders
// distinct content — if the teleport regresses or the tab buttons get hidden behind the
// backdrop again, this test will fail at the first click.
test("Query JSON modal: each tab renders distinct content", async ({ page }) => {
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

	// Pick one column + one measure so the JSON has groupBy / measures content to inspect.
	await page.getByRole("button", { name: /columns/ }).click();
	await page.locator("input[id='column_country']").check();
	await page.getByRole("button", { name: /measures/ }).click();
	await page.locator("input[id='measure_delta']").check();

	// Open the modal. The trigger sits inside the (potentially floating) Submit block.
	await page.getByRole("button", { name: /^JSON$/ }).click();

	const modal = page.locator("#queryJsonRaw");
	await expect(modal).toBeVisible();

	// JSON tab is active by default. The pre block must contain at least one of the
	// columns we selected (proves the model serialised end-to-end).
	const jsonPane = modal.locator("[data-adhoc-tab-pane='json']");
	await expect(jsonPane).toBeVisible();
	await expect(jsonPane).toContainText("country");
	await expect(jsonPane).toContainText("delta");

	// MDX tab — derived projection. Must mention the SELECT / FROM keywords MDX uses.
	await modal.locator("[data-adhoc-tab='mdx']").click();
	const mdxPane = modal.locator("[data-adhoc-tab-pane='mdx']");
	await expect(mdxPane).toBeVisible();
	await expect(mdxPane).toContainText(/SELECT/i);

	// SQL tab — derived projection. Must mention SELECT / FROM.
	await modal.locator("[data-adhoc-tab='sql']").click();
	const sqlPane = modal.locator("[data-adhoc-tab-pane='sql']");
	await expect(sqlPane).toBeVisible();
	await expect(sqlPane).toContainText(/SELECT/i);

	// Mermaid tab — must render an SVG (mermaid.render writes to mermaidSvg via v-html).
	await modal.locator("[data-adhoc-tab='mermaid']").click();
	const mermaidPane = modal.locator("[data-adhoc-tab-pane='mermaid']");
	await expect(mermaidPane).toBeVisible();
	await expect(mermaidPane.locator("svg")).toBeVisible();

	// Back to JSON to assert the tab toggle is reversible.
	await modal.locator("[data-adhoc-tab='json']").click();
	await expect(modal.locator("[data-adhoc-tab-pane='json']")).toBeVisible();

	// Close cleanly so the test leaves the page in a sane state for any follow-up.
	await modal.getByRole("button", { name: "Close" }).first().click();
	await expect(modal).toBeHidden();
});
