import { test, expect } from "@playwright/test";

import queryPivotable from "./query-pivotable.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// Regression test for the BASIC-in-modal bug.
//
// When the SPA loginModal is open (e.g. after a session expiry) and the user clicks
// the BASIC provider, the BASIC username/password form must render IN-PLACE inside
// the modal. It must NOT trigger a full-page navigation to `/html/login/basic`,
// which would discard the current view and break the "smooth re-login" UX.
//
// Root cause of the original bug (fixed in login-providers.js): the modal branch
// was gated on `item.registration_id == 'BASIC'`, but the backend sets
// registration_id to the profile name (e.g. `pivotable-unsafe_fakeuser`), so the
// gate never matched and the template fell through to `<a :href="item.login_url">`
// which navigated to /html/login/basic. Fix: gate on `item.type == 'basic'`.
test("BASIC login stays inside loginModal (not full-page redirect)", async ({ page }) => {
	// Reuse the 401-injection pattern from localhost8080-token-expiry to open the
	// modal mid-session. We whitelist /api/login/v1/* so that the subsequent
	// CSRF + BASIC login calls still work if the test wants to complete the flow.
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

	// Sanity: we are on a query page with data visible.
	await expect(page.locator(".slick-row").first()).toBeVisible();
	const urlBeforeExpiry = page.url();

	// Arm 401 injection and fire an API call that triggers the "needsToLogin" flip.
	expireNext = true;
	await queryPivotable.addColumn(page, "Shirt Number");

	const modal = page.locator("#loginModal");
	await expect(modal).toBeVisible();
	await expect(page.getByRole("heading", { name: "Login to Pivotable" })).toBeVisible();

	// Click the BASIC entry inside the modal. Before the fix, this was an
	// `<a href="/html/login/basic">` → full-page navigation. After the fix, it is
	// a `<a href="#" @click.prevent>` that toggles `selectedProvider` locally.
	await modal.getByRole("link", { name: /pivotable-unsafe_fakeuser/ }).click();

	// The BASIC form renders in-place inside the modal.
	await expect(modal.getByRole("button", { name: /Login fakeUser/ })).toBeVisible();

	// And we did NOT navigate to /html/login/basic — the view underneath is
	// preserved so the user can resume whatever they were doing after re-login.
	expect(page.url()).toBe(urlBeforeExpiry);
});
