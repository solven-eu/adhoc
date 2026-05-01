import { test, expect } from "@playwright/test";

import queryPivotable from "./query-pivotable.mjs";

// E2E for the endpoint-schema page (/html/endpoints/<id>/schema). The page replaced an
// earlier flat triple-list with a Bootstrap three-section accordion (Tables / Measure
// bags / Cubes) plus per-section filter inputs. This spec exercises the structural
// changes:
//   1. The three section headers render with non-zero count badges on the default
//      `simple`-bearing endpoint.
//   2. Cubes is expanded by default; Tables and Measure bags are collapsed.
//   3. Expanding a section reveals a filter input that narrows the visible rows.
//
// We reach the page via the in-app `Show schema` link rather than navigating directly,
// so the test also doubles as a regression for that link.

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

test("endpoint schema page renders three accordion sections with filters", async ({ page }) => {
	await page.goto(url);
	await queryPivotable.login(page);
	await page.getByRole("link", { name: "Browse through endpoints" }).click();

	// Click the `Show schema` link rendered by `AdhocEndpointSchemaChip`. There may be
	// more than one endpoint shown if `/?cdn` registered extras; pick the first.
	await page
		.getByRole("link", { name: /Show schema/i })
		.first()
		.click();
	await expect(page).toHaveURL(/\/html\/endpoints\/.+\/schema$/);

	// Section headers — each label is followed by a count badge. Use a regex on the
	// ACCESSIBLE NAME (button text) so the assertion does not fragilely depend on the
	// exact whitespace between the label and the badge.
	const tablesHeader = page.getByRole("button", { name: /Tables\s*\d+/ });
	const measuresHeader = page.getByRole("button", { name: /Measure bags\s*\d+/ });
	const cubesHeader = page.getByRole("button", { name: /Cubes\s*\d+/ });

	await expect(tablesHeader).toBeVisible();
	await expect(measuresHeader).toBeVisible();
	await expect(cubesHeader).toBeVisible();

	// Cubes default to expanded — its accordion-button should NOT have the `collapsed`
	// class, while Tables and Measures should. Bootstrap toggles this class on click,
	// so it is the load-bearing assertion for the open/closed default.
	await expect(tablesHeader).toHaveClass(/collapsed/);
	await expect(measuresHeader).toHaveClass(/collapsed/);
	await expect(cubesHeader).not.toHaveClass(/collapsed/);

	// Cubes section is open — a cube link is visible (the `simple` cube is part of the
	// default fixture, registered by `InjectSimpleExampleCubesConfig`).
	await expect(page.getByRole("link", { name: /simple/i }).first()).toBeVisible();

	// Filter the Cubes list. The placeholder is unique enough to target.
	const cubeFilter = page.getByPlaceholder("Filter cubes…");
	await cubeFilter.fill("simple");
	await expect(page.getByRole("link", { name: /simple/i }).first()).toBeVisible();

	// A bogus filter should hide all cubes (the list-group is rendered with one <li>
	// per cube, and our v-if drops non-matching ones — so no link with anything
	// remotely cube-shaped should remain visible).
	await cubeFilter.fill("zzz_no_such_cube");
	await expect(page.getByRole("link", { name: /^simple$/i })).toHaveCount(0);
	await cubeFilter.fill("");

	// Open the Tables section and assert it surfaces at least one table row + the
	// filter input.
	await tablesHeader.click();
	await expect(tablesHeader).not.toHaveClass(/collapsed/);
	await expect(page.getByPlaceholder("Filter tables…")).toBeVisible();

	// Open the Measure bags section and the same filter input check.
	await measuresHeader.click();
	await expect(measuresHeader).not.toHaveClass(/collapsed/);
	await expect(page.getByPlaceholder(/Filter measures/)).toBeVisible();
});
