import { test, expect } from "@playwright/test";

import queryPivotable from "./query-pivotable.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// E2E coverage for the measure-graph modal on the `simple` cube.
//
// The measure-graph modal is opened by clicking the `?` badge next to a selected measure
// (either on the wizard entry or the grid header). The modal renders a Mermaid graph of
// the measure and its dependencies/dependants. Clicking a node in the graph calls
// `window.clickAddMeasure(name)`, which:
//   1. Adds the measure to `queryModel.selectedMeasures` (so the wizard switch flips on
//      and a new column appears in the grid).
//   2. Accumulates the click into the modal's own `clickedMeasures` ref so the rendered
//      graph expands to include both the original measure and the newly-clicked one.
test("Measure-graph modal: clicking a linked measure adds it to the query and extends the graph", async ({ page }) => {
	await page.goto(url);

	await page.getByRole("link", { name: /You need to login/ }).click();
	await page.getByRole("link", { name: "pivotable-unsafe_fakeuser" }).click();
	await page.getByRole("button", { name: "Login fakeUser" }).click();

	await page.getByRole("link", { name: "Browse through endpoints" }).click();
	await page
		.getByRole("link", { name: /simple/i })
		.first()
		.click();
	await page.getByRole("link", { name: /Query simple/i }).click();

	// Start from the combinator `delta+gamma` — it underlies `delta` and `gamma`, so the
	// graph will have clickable child nodes to exercise.
	//
	// The accessible name of the switch (label text) includes the `?` badge and tag chips,
	// so we target the checkbox by its stable `id="measure_<name>"` attribute. `+` is a CSS
	// special character, hence the attribute-selector form rather than `#measure_delta\\+gamma`.
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("delta+gamma");
	await page.getByRole("switch", { name: "JSON" }).uncheck();
	await page.getByRole("button", { name: /measures/ }).click();
	await page.locator("input[id='measure_delta+gamma']").check();

	// Wait for the resulting grid to render so we know the query went through.
	await expect(page.locator(".slick-row").first()).toBeVisible();

	// Open the measure modal via the `?` badge next to `delta+gamma` in the wizard. The badge
	// is rendered as `<span class="badge text-bg-primary">` (not a real <button>) — sibling
	// `.badge.text-bg-secondary` chips carry the measure's tags (δ, γ), so we target the
	// primary-styled badge specifically to avoid ambiguity.
	await page.locator("label[for='measure_delta+gamma'] .badge.text-bg-primary").click();

	// Modal visible.
	const modal = page.locator("#measureDag");
	await expect(modal).toBeVisible();

	// Graph rendered. Mermaid emits SVG <g class="node"> elements. At minimum, the node for
	// the main measure (`delta+gamma`) must be there.
	const nodes = modal.locator("pre.mermaid g.node");
	await expect(nodes.first()).toBeVisible();

	// Find the node labelled `delta` (an underlying of `delta+gamma`) and click it. The
	// mermaid node's visible <text> contains the measure name — use a :has selector so we
	// click the <g class="node"> whose text is exactly `delta`.
	const deltaNode = modal
		.locator("g.node")
		.filter({ hasText: /^\s*delta\s*$/ })
		.first();
	await expect(deltaNode).toBeVisible();
	await deltaNode.click();

	// EXPECTED 1 (modal side): the graph re-rendered to include BOTH measures as first-class
	// nodes with the user-selected styling. Checked BEFORE closing the modal because once
	// closed the SVG is no longer in the DOM.
	await expect(modal).toBeVisible();
	await expect(
		modal
			.locator("g.node")
			.filter({ hasText: /^\s*delta\s*$/ })
			.first(),
	).toBeVisible();
	await expect(
		modal
			.locator("g.node")
			.filter({ hasText: /^\s*delta\+gamma\s*$/ })
			.first(),
	).toBeVisible();

	// Close the modal so we can interact with the wizard sidebar (the backdrop otherwise
	// intercepts clicks).
	await modal.getByRole("button", { name: "Close" }).first().click();
	await expect(modal).toBeHidden();

	// EXPECTED 2 (queryModel side): `delta` is now a selected measure. The search filter has
	// to be widened to make the corresponding switch visible — it was previously narrowed to
	// "delta+gamma" which would not list `delta` in the accordion body.
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("delta");
	await expect(page.locator("input[id='measure_delta']")).toBeChecked();
	await expect(page.locator("input[id='measure_delta+gamma']")).toBeChecked();
});
