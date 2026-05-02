import { test, expect, request } from "@playwright/test";

import queryPivotable from "./query-pivotable.mjs";

// Regression test for the inline copy-name icon next to each column header. The bug:
// clicking the icon both copied the name to the clipboard AND triggered SlickGrid's
// column-sort handler, because SlickGrid binds its sort listener on
// `.slick-header-column` and our delegated handler used to live in BUBBLE phase, so
// the sort fired before stopPropagation could run. The fix uses CAPTURE phase — see
// `adhoc-query-grid-clipboard.js` and the unit test in `query-grid-clipboard.spec.js`.
//
// The unit test only exercises the pure helper. This e2e test is the real-world
// guarantee: it drives a click on the icon in a real browser against a real
// SlickGrid instance, and asserts the column ordering is unchanged.

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

test("copyToClipboard icon does NOT trigger column sort", async ({ page, context }) => {
	// Required for `navigator.clipboard.writeText` to resolve under Playwright (otherwise
	// it rejects on permission). Failing to grant the permission would force the helper
	// onto its execCommand fallback, which still works but muddies the test.
	await context.grantPermissions(["clipboard-read", "clipboard-write"]);

	await page.goto(url);
	await queryPivotable.queryPivotable(page);

	// Snapshot the values of the first groupBy column BEFORE the click. We pick the
	// `Position` column which has at least two distinct values in the WorldCupPlayers
	// fixture, so a sort flip would change the row ordering visibly.
	const positionColumnIndex = 1;
	const valuesBefore = await page.locator(".slick-row").locator(".slick-cell").nth(positionColumnIndex).allTextContents();

	// Click the inline clipboard icon attached to the `Position` header. The icon is
	// rendered as a `<i class="bi bi-clipboard adhoc-copy-name-btn">` inside the
	// header cell.
	const positionHeader = page.locator(".slick-header-column").filter({ hasText: "Position" });
	await positionHeader.locator(".adhoc-copy-name-btn").click();

	// Clipboard must contain the column name. Reading the clipboard requires the
	// permission granted above and only works on Chromium under Playwright.
	const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
	expect(clipboardContent).toBe("Position");

	// Critically — the row ordering must NOT have changed. If the sort fired, the
	// values would have been re-ordered alphabetically (or by reverse). The original
	// order is preserved when stopPropagation correctly intercepts the click in
	// capture phase.
	const valuesAfter = await page.locator(".slick-row").locator(".slick-cell").nth(positionColumnIndex).allTextContents();
	expect(valuesAfter).toEqual(valuesBefore);

	// Belt-and-braces: the SlickGrid sort indicator should NOT be set. SlickGrid adds
	// `.slick-header-column-sorted` to a column header once it has been used as the
	// active sort key.
	await expect(positionHeader).not.toHaveClass(/slick-header-column-sorted/);
});
