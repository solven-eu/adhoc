// @ts-check
import { test, expect } from "./_coverage-fixture.mjs";

import queryPivotable from "./query-pivotable.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// Regression test for: after a token expiry + in-place BASIC re-login, the URL still carries
// the encoded queryModel (column / measure selection) but the grid is empty because the
// queryModel was NOT re-hydrated from the hash on the remounted <AdhocQuery> instance.
//
// Root cause (history-of-the-bug): adhoc-query.js updates the URL via `history.pushState(...)`
// every time the user edits the model. pushState bypasses vue-router, so its reactive
// `currentRoute.value.hash` falls out of sync with the real URL. When <AdhocQuery> unmounts
// (the wrapper swaps in <LoginChip>) and remounts after re-login, the setup reads the hash via
// `router.currentRoute.value.hash` — getting vue-router's STALE value, often empty. The result:
// the URL still has the queryModel, but the in-memory model is empty.
//
// Fix: read from `window.location.hash` directly on the initial-hydration path (the authoritative
// source). This test verifies both the URL preservation AND that the grid actually re-runs the
// previously-built query after re-login.
test("After re-login, the queryModel is re-hydrated from the URL hash and the grid repopulates", async ({ page }) => {
	// Arm a 401 injection that we can flip on/off mid-test, mirroring the pattern in
	// localhost8080-token-expiry.spec.js / localhost8080-login-modal-basic.spec.js so the test
	// doesn't depend on a real backend-side token-expiry profile.
	let expireNext = false;
	await page.route("**/api/**", async (route) => {
		if (!expireNext) {
			return route.continue();
		}
		const u = route.request().url();
		// Whitelist the fixture-clear endpoint and the login routes so re-login itself can
		// complete while every functional /api/** call is forced to 401.
		if (u.includes("/api/v1/clear") || u.includes("/api/login/v1/")) {
			return route.continue();
		}
		return route.fulfill({
			status: 401,
			contentType: "application/json",
			body: JSON.stringify({ error: "access_token expired (simulated)" }),
		});
	});

	await page.goto(url);
	await queryPivotable.queryPivotable(page);

	// Sanity: grid is populated and the URL hash now encodes the model (column + measure).
	await expect(page.locator(".slick-row").first().locator(".slick-cell").nth(2)).toBeVisible();
	const valueCellBeforeExpiry = await page.locator(".slick-row").first().locator(".slick-cell").nth(2).textContent();
	expect(valueCellBeforeExpiry, "pre-expiry: grid carries a numeric measure value").toMatch(/\d/);

	const urlBeforeExpiry = page.url();
	// The hash MUST be present, otherwise the test is degenerate (nothing to re-hydrate from).
	expect(urlBeforeExpiry.indexOf("#")).toBeGreaterThan(0);
	const hashBeforeExpiry = urlBeforeExpiry.substring(urlBeforeExpiry.indexOf("#"));
	expect(hashBeforeExpiry.length).toBeGreaterThan(1);

	// ── Simulate expiry ──
	// Same recipe as localhost8080-login-modal-basic: arm the 401, drop cookies so the
	// silent /oauth2/token refresh also 401s, then trigger any re-fetch.
	expireNext = true;
	await page.context().clearCookies();
	await queryPivotable.addColumn(page, "Shirt Number");

	// ── loginModal opens ──
	const modal = page.locator("#loginModal");
	await expect(modal).toBeVisible({ timeout: 10_000 });

	// ── Complete BASIC re-login ──
	await modal.getByRole("link", { name: /pivotable-unsafe_fakeuser/ }).click();
	const loginButton = modal.getByRole("button", { name: /^Login$/i });
	await expect(loginButton).toBeVisible();
	// Disarm the 401 BEFORE clicking Login so the post-login /api/** traffic succeeds.
	expireNext = false;
	await loginButton.click();
	await expect(modal).toBeHidden();

	// ── The actual regression assertion ──
	// (a) URL still carries the queryModel — should ALWAYS be true; not the bug.
	expect(page.url().indexOf("#")).toBeGreaterThan(0);

	// (b) The grid re-populates from that URL hash. This is the load-bearing claim: before the
	// fix, the queryModel on the remounted <AdhocQuery> was empty, so the grid had no rows.
	// Wait specifically for a numeric cell — the empty-model state would render nothing here.
	await expect(page.locator(".slick-row").first().locator(".slick-cell").nth(2)).toBeVisible({ timeout: 15_000 });
	const valueCellAfterRelogin = await page.locator(".slick-row").first().locator(".slick-cell").nth(2).textContent();
	expect(valueCellAfterRelogin, "post-relogin: grid carries a numeric measure value (model hydrated from URL hash)").toMatch(/\d/);
});
