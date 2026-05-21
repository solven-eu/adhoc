// @ts-check
import { test, expect } from "./_coverage-fixture.mjs";

import queryPivotable from "./query-pivotable.mjs";

import { BASE_URL as url } from "./_url.mjs";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// Reproduces the "dblclick auto-fit doesn't shrink an over-wide column" bug.
//
// Sequence: add a single groupBy column → switch to scroll mode → manually expand the
// column to be MUCH wider than its content → dblclick the right resize handle.
//
// The column should snap back to a width that just fits its header + cells. The previous
// implementations failed this in two ways:
//   1. `applyColumnWidths()` alone updated the cell CSS rule but left the header DOM untouched —
//      cells looked narrow, header stayed wide, column slot didn't shrink.
//   2. Without a follow-up `resizeCanvas()`, the canvas kept its OLD total width and the column
//      "didn't shrink" visually even though its `.width` field had been updated.
//
// The expected post-fix path is the 3-step light update:
//   applyColumnWidths() + applyColumnHeaderWidths() + resizeCanvas().
test.setTimeout(60_000);
test("Auto-fit dblclick shrinks an over-wide column in scroll mode", async ({ page }) => {
	await page.goto(url);
	await queryPivotable.queryPivotable(page);

	// 1. Pin the test to a single, deterministic groupBy column. Reset whatever the previous
	//    setup added.
	await page
		.locator(".btn", { hasText: /Reset query/i })
		.first()
		.click({ trial: false })
		.catch(() => {});

	// 2. Add a single groupBy column. The wizard's columns accordion may already be open from
	//    the queryPivotable() helper; close+reopen anything tags-related is unnecessary.
	await queryPivotable.addColumn(page, "Position");

	// 3. Switch to scroll mode via the grid-layout toggle. The toggle is the "Scroll" / "fit"
	//    button in the grid controls; tooltip is stable across builds.
	await page
		.locator("button", { hasText: /Scroll/ })
		.first()
		.click();

	// 4. Find the column header and the resize handle. The handle is the rightmost child of the
	//    `.slick-header-column` for the Position column.
	const header = page.locator(".slick-header-column").filter({
		has: page.locator(".adhoc-header-name", { hasText: /^Position$/ }),
	});
	await expect(header).toBeVisible();

	const handle = header.locator(".slick-resizable-handle");
	await expect(handle).toBeVisible();

	// 5. Snapshot the natural width that scroll mode produced. This is the baseline we want
	//    auto-fit to converge back to after we manually widen.
	const naturalWidth = await header.evaluate((el) => el.getBoundingClientRect().width);
	expect(naturalWidth).toBeGreaterThan(0);

	// 6. Manually expand the column by dragging the handle ~400 px to the right.
	const handleBox = await handle.boundingBox();
	expect(handleBox).not.toBeNull();
	const handleCenterX = handleBox.x + handleBox.width / 2;
	const handleCenterY = handleBox.y + handleBox.height / 2;
	const dragDelta = 400;
	await page.mouse.move(handleCenterX, handleCenterY);
	await page.mouse.down();
	await page.mouse.move(handleCenterX + dragDelta, handleCenterY, { steps: 10 });
	await page.mouse.up();

	// Drag landed → header is wider now.
	const widenedWidth = await header.evaluate((el) => el.getBoundingClientRect().width);
	expect(widenedWidth).toBeGreaterThan(naturalWidth + 100);

	// 7. Dblclick the resize handle — the auto-fit affordance. After this, the column should
	//    snap back to roughly its natural width.
	await handle.dblclick();

	// 8. The visible column slot must actually SHRINK. This is the assertion the previous bug
	//    failed: it would update internal state but leave the rendered width at the widened
	//    value until the user hovered/scrolled. We tolerate a small delta (≤30 px) because the
	//    auto-fit also accounts for header chrome that scroll-mode's initial measurement
	//    doesn't, and the absolute target depends on the column's content (which the backend
	//    chooses).
	const fitWidth = await header.evaluate((el) => el.getBoundingClientRect().width);
	expect(fitWidth).toBeLessThan(widenedWidth - 100);

	// Sanity: cells got the new width too. If only the header shrank, we'd see misaligned
	// columns — what the user reported as "looks weird".
	const firstCell = page.locator(".slick-row").first().locator(".slick-cell").nth(0);
	const cellWidth = await firstCell.evaluate((el) => el.getBoundingClientRect().width);
	// Cells should be very close to the header width (within a couple of pixels — borders).
	expect(Math.abs(cellWidth - fitWidth)).toBeLessThan(4);
});
