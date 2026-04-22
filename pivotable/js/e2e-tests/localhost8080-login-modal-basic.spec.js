import { test, expect } from "@playwright/test";

import queryPivotable from "./query-pivotable.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// Regression test for the BASIC-in-modal bug + full "smooth re-login" UX.
//
// Motivation: when the user is on a query view and their JWT access_token expires,
// the SPA opens the loginModal on top of the current view. The user should be able
// to re-login IN-PLACE and resume exactly where they were — same URL, same grid,
// same values. The original bug (fixed in login-providers.js) triggered a full-page
// navigation to `/html/login/basic` as soon as the user picked BASIC, wiping the
// view.
//
// Root cause: the modal branch was gated on `item.registration_id == 'BASIC'`, but
// the backend sets registration_id to the profile name (e.g.
// `pivotable-unsafe_fakeuser`), so the gate never matched and the template fell
// through to `<a :href="item.login_url">`. Fix: gate on `item.type == 'basic'`.
test("BASIC re-login after token expiry stays in modal and preserves the view", async ({ page }) => {
	// Arm 401 injection AFTER we reach the query view. Flip `expireNext = true` to
	// make the next functional /api/** call return 401 — triggering the SPA's
	// silent refresh (also 401) → `needsToLogin = true` → loginModal opens.
	// Whitelist /api/v1/clear (test fixture) and /api/login/v1/* (so the real
	// BASIC login inside the modal can still complete).
	let expireNext = false;
	await page.route("**/api/**", async (route) => {
		if (!expireNext) {
			return route.continue();
		}
		const u = route.request().url();
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

	// ── Snapshot the view the user will be re-entering into after re-login ──
	// At least one figure must be visible; we remember the URL and the first
	// cell's text so we can assert the same view is still there after re-login.
	const firstCell = page.locator(".slick-row").first().locator(".slick-cell").nth(2);
	await expect(firstCell).toBeVisible();
	const firstCellBeforeExpiry = await firstCell.textContent();
	const urlBeforeExpiry = page.url();
	expect(firstCellBeforeExpiry, "pre-expiry: at least one figure rendered").toMatch(/\d/);

	// ── Simulate expiry ──
	expireNext = true;
	await queryPivotable.addColumn(page, "Shirt Number");

	// ── Modal opens on top of the query view ──
	const modal = page.locator("#loginModal");
	await expect(modal).toBeVisible();
	await expect(page.getByRole("heading", { name: "Login to Pivotable" })).toBeVisible();

	// Clicking BASIC must NOT navigate — regression guard for the original bug.
	await modal.getByRole("link", { name: /pivotable-unsafe_fakeuser/ }).click();
	expect(page.url(), "clicking BASIC in modal must not navigate").toBe(urlBeforeExpiry);

	// BASIC form renders in-place inside the modal.
	const loginButton = modal.getByRole("button", { name: /Login fakeUser/ });
	await expect(loginButton).toBeVisible();

	// ── Complete the re-login ──
	// Disarm the 401 injection so the post-login /api/** traffic (token mint,
	// data refresh) succeeds. The credentials are the ones pre-filled by
	// login-basic.js for the fakeUser.
	expireNext = false;
	await loginButton.click();

	// Modal closes once `needsToLogin` flips back to false.
	await expect(modal).toBeHidden();

	// ── Assert the view survived: same URL, same first cell value ──
	expect(page.url(), "URL preserved across re-login").toBe(urlBeforeExpiry);
	await expect(page.locator(".slick-row").first().locator(".slick-cell").nth(2)).toHaveText(firstCellBeforeExpiry);
});
