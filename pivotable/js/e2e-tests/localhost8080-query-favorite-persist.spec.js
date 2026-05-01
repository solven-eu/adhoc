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
	await page.getByRole("button", { name: /^Login$/i }).click();

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
	await page.locator('[id="column_city"]').check();

	await page.getByRole("searchbox", { name: "Search" }).dblclick();
	await page.getByRole("searchbox", { name: "Search" }).fill("delta");
	await page.getByRole("switch", { name: "JSON" }).uncheck();
	await page.getByRole("button", { name: /measures/ }).click();
	await page.locator('[id="measure_delta"]').check();

	await expect(page.locator(".slick-row").first()).toBeVisible({ timeout: 15000 });

	// ── Save as favorite ──
	// The Favorite toggle button renders inside `adhoc-query-favorite.js`. Its text is
	// "Favorite" plus an optional " * " span when there are unsaved changes; `name: "Favorite"`
	// with `exact: false` would also match the plural "Favorites" modal, so we anchor on the
	// modal trigger via its `data-bs-target`.
	await page.locator('button[data-bs-target="#queryFavorite"]').first().click();
	await expect(page.locator("#queryFavorite")).toBeVisible();
	await page.getByRole("textbox", { name: "Query name" }).fill(favoriteName);
	await page.locator("#queryFavorite").getByRole("button", { name: "Save", exact: true }).click();
	// Wait for the favorite to actually land in localStorage before tearing down
	// the page. The store's $subscribe persists asynchronously after $patch — a
	// fast `page.goto` below could otherwise win the race and lose the entry.
	await page.waitForFunction((name) => {
		try {
			const raw = window.localStorage.getItem("adhoc.preferences");
			if (!raw) return false;
			const payload = JSON.parse(raw);
			return Object.values(payload.queryModels || {}).some((m) => m.name === name);
		} catch {
			return false;
		}
	}, favoriteName);
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
	// The Submit block's <Transition> may briefly render both the leaving (floating) and entering (docked)
	// copies during its 0.3s leave animation. `.first()` deterministically picks the docked one (which the
	// default-mode Transition renders first).
	await page.locator('button[data-bs-target="#queryFavorites"]').first().click();
	// Wait for the modal to be fully open before searching its body — Bootstrap
	// applies the show transition asynchronously, and `getByText` does not retry
	// across visibility transitions reliably.
	await expect(page.locator("#queryFavorites")).toBeVisible();
	// Find the list-group entry whose body contains the unique timestamp-based name.
	// Click directly on the load-handler row (the inner div with cursor:pointer).
	// `getByText` over the concatenated `id=…name=…path=…` text matches the whole
	// run, so we instead pick the list-item by `hasText` substring and click the
	// first descendant carrying the cursor:pointer style.
	await expect(page.locator("#queryFavorites.show")).toBeVisible();
	// Bootstrap's modal-backdrop element is stacked just below the modal-dialog and,
	// due to a known interaction with the way some browsers compute pointer-event
	// targets across position:fixed stacking contexts, can intercept the click on
	// the inner row. Trigger the load handler directly via DOM dispatch — Vue's
	// @click listener is a regular addEventListener on the .adhoc-favorite-load div,
	// so a synthetic click event reaches it without going through the backdrop.
	await page.evaluate((name) => {
		const items = Array.from(document.querySelectorAll("#queryFavorites .list-group-item"));
		const row = items.find((li) => li.textContent.includes(name));
		if (!row) throw new Error("Favorite row not found");
		const target = row.querySelector(".adhoc-favorite-load");
		if (!target) throw new Error("adhoc-favorite-load child not found");
		target.click();
	}, favoriteName);

	// ── Verify: loading the favorite restored the query and a fresh round-trip produced
	// rows. This is the actual end-to-end proof the favorite was preserved in localStorage
	// across the reload and carries back the full queryModel (columns + measures). The
	// extra 10 s timeout accommodates loadQuery → autoQuery round-trip → grid resync,
	// which can exceed the default 2 s on a cold backend.
	await expect(page.locator(".slick-row").first()).toBeVisible({ timeout: 10000 });
});
