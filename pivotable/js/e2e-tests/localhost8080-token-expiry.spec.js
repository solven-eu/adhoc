import { test, expect } from "@playwright/test";

import queryPivotable from "./query-pivotable.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// Sanitize the behavior on access_token expiry: we EXPECT the bootstrap loginModal
// (id=loginModal, title "Login to Pivotable") to open, and the user to stay on
// the query page. If instead the app performs a full-page redirect to the login
// page, this test will fail and pinpoint the regression.
//
// Technique: intercept API calls with page.route and force a 401 on the next
// data call AND on the subsequent /oauth2/token refresh. The double 401 is
// required because store-user.js:296 reacts to `access_token_expired` by calling
// forceLoadUserTokens(); only if that refresh *also* fails does `needsToLogin`
// flip to true (store-user.js:270) and the loginModal open.
test("loginModal opens when access_token expires mid-session", async ({ page }) => {
	// Arm the 401 injection AFTER the login + first query, so the initial flow
	// behaves normally. We flip `expireNext` to true once we're ready.
	let expireNext = false;

	await page.route("**/api/**", async (route) => {
		if (!expireNext) {
			return route.continue();
		}
		const requestUrl = route.request().url();
		// Expire the next data call and any subsequent oauth2/token refresh so
		// that forceLoadUserTokens() also fails and needsToLogin becomes true.
		if (requestUrl.includes("/api/v1/clear")) {
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

	// Simulate expiry: every future API call (including the /oauth2/token
	// refresh attempt) returns 401.
	expireNext = true;

	// Trigger any action that fires an API call. Adding a column re-runs the
	// query through the same wrapper that dispatches `needsToLogin` on failure.
	await queryPivotable.addColumn(page, "Shirt Number");

	// EXPECTED: the bootstrap loginModal opens in-place.
	await expect(page.locator("#loginModal")).toBeVisible();
	await expect(page.getByRole("heading", { name: "Login to Pivotable" })).toBeVisible();

	// EXPECTED: no full-page redirect away from the query page.
	expect(page.url()).toBe(urlBeforeExpiry);
});
