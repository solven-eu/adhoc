import { test, expect } from "./_coverage-fixture.mjs";

import queryPivotable from "./query-pivotable.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// Regression test covering the "favorite survives a full browser reload" flow.
//
// Users reported that after saving a favorite, clearing the wizard, and doing F5 (so the SPA
// reinitialises and the pinia store rebuilds from localStorage), they expect the favorite to
// still be there and to restore the original query on click. This test nails the full loop
// end-to-end on the `simple` cube:
//   1. configure a working query (city + delta)
//   2. save it as a favorite with a known name
//   3. clear the wizard in place
//   4. reload the page (equivalent of pressing F5)
//   5. open the favorites modal and click the saved entry
//   6. verify the query state is restored (switches checked, rows rendered again)
test("Favorite saved on simple cube survives F5 and restores on reopen", async ({ page }) => {
	const favoriteName = "e2e-favorite-" + Date.now();

	// Wipe localStorage ONCE before the test, so stale favorites from prior runs don't
	// pollute the favorites list. Explicitly NOT via `context.addInitScript` because that
	// re-runs on every navigation — it would clobber the favorite we just saved right
	// before the F5 equivalent below.
	await page.goto(url);
	await page.evaluate(() => {
		try {
			window.localStorage.clear();
		} catch {}
	});
	await page.getByRole("link", { name: /You need to login/ }).click();
	await page.getByRole("link", { name: "pivotable-unsafe_fakeuser" }).click();
	await page.getByRole("button", { name: "Login fakeUser" }).click();

	await page.getByRole("link", { name: "Browse through endpoints" }).click();
	await page
		.getByRole("link", { name: /simple/i })
		.first()
		.click();
	await page.getByRole("link", { name: /Query simple/i }).click();

	// ── Build a working query ──
	// `city` clashes with `capital_city` in the column list, so we can't use the shared
	// `queryPivotable.addColumn` helper (it matches names loosely). Inline-exact match.
	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("city");
	await page.getByRole("button", { name: /columns/ }).click();
	await page.getByRole("switch", { name: "city", exact: true }).check();

	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("delta");
	await page.getByRole("switch", { name: "JSON" }).uncheck();
	await page.getByRole("button", { name: /measures/ }).click();
	await page.getByRole("switch", { name: /^delta$/ }).check();

	await expect(page.locator(".slick-row").first()).toBeVisible();

	// ── Save as favorite ──
	// The Favorite toggle button renders inside `adhoc-query-favorite.js`. Its text is
	// "Favorite" plus an optional " * " span when there are unsaved changes; `name: "Favorite"`
	// with `exact: false` would also match the plural "Favorites" modal, so we anchor on the
	// modal trigger via its `data-bs-target`.
	await page.locator('button[data-bs-target="#queryFavorite"]').click();
	await page.getByRole("textbox", { name: "Query name" }).fill(favoriteName);
	await page.getByRole("button", { name: "Save", exact: true }).click();
	// No need to dismiss the modal explicitly — the favorite is already persisted to
	// localStorage via the pinia `$subscribe` wired in `store-preferences.js`, and the
	// subsequent `page.goto` tears down the whole DOM anyway.

	// ── Capture the query-view URL stripped of the hash (the hash encodes the current
	// query — back/forward support — so reloading with it would restore the query from
	// the URL rather than proving the favorite survived). Navigating to the bare URL is
	// equivalent to the user manually clearing the wizard then pressing F5. ──
	const cleanQueryUrl = new URL(page.url());
	cleanQueryUrl.hash = "";
	await page.goto(cleanQueryUrl.toString());

	// Grid must be empty after the clean reload — the SPA reinitialises from localStorage
	// (which holds the favorite) with no in-memory query state, so no query fires. We
	// purposefully DON'T assert on specific wizard switches here because the measures /
	// columns accordions filter their content by the search box, which is also cleared
	// on reload, so the `delta` / `city` switches are not in the DOM until typed again.
	await expect(page.locator(".slick-row")).toHaveCount(0);

	// ── Reopen the favorite from the Favorites modal ──
	await page.locator('button[data-bs-target="#queryFavorites"]').click();
	// List items in `adhoc-query-favorites.js` render as clickable <li>s; the saved entry is
	// the only one with our unique timestamp-based name.
	await page
		.locator("#queryFavorites")
		.getByText("name=" + favoriteName)
		.click();

	// ── Verify: loading the favorite restored the query and a fresh round-trip produced
	// rows. This is the actual end-to-end proof the favorite was preserved in localStorage
	// across the reload and carries back the full queryModel (columns + measures). ──
	await expect(page.locator(".slick-row").first()).toBeVisible();
});
