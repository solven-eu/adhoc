import { test, expect } from "@playwright/test";

// Verifies each (cdn/webjars) × (dev/min) combination boots cleanly.
//
// The cross-product is wired into `index.html`'s bootstrap block:
//   - default (no flags) → /ui/importmap-webjars-min.json
//   - ?cdn               → /ui/importmap-cdn-min.json
//   - ?dev               → /ui/importmap-webjars.json
//   - ?cdn&dev           → /ui/importmap-cdn.json
// All four must end up on the same SPA shell — same title, same first-route content
// (HelloView's "Welcome to Pivotable (Adhoc)" heading), no console errors.
//
// Why this test exists: a regression in the bootstrap (e.g. mis-tokenised `<script>`
// strings, importmap not registered before module-script execution, or a duplicated
// vue runtime under `?cdn` in dev mode) silently crashes the SPA on first render and
// shows nothing but the splash. Without an automated check, the regression escapes to
// production.
const url = "http://localhost:8080";

const COMBINATIONS = [
	{ name: "default (webjars + min)", search: "" },
	{ name: "cdn + min", search: "?cdn" },
	{ name: "webjars + dev", search: "?dev" },
	{ name: "cdn + dev", search: "?cdn&dev" },
];

for (const { name, search } of COMBINATIONS) {
	test(`SPA boots cleanly: ${name}`, async ({ page }) => {
		const consoleErrors = [];
		const pageErrors = [];
		page.on("console", (msg) => {
			if (msg.type() === "error") consoleErrors.push(msg.text());
		});
		page.on("pageerror", (err) => pageErrors.push(err.message));

		await page.goto(url + "/" + search);

		// Splash is removed by the inline module script after `app.mount()` resolves —
		// its absence proves the SPA mounted. Use a generous timeout because under
		// `?dev` modules are unminified and the first-render cost is higher.
		await expect(page.locator("#adhoc-splash")).toHaveCount(0, { timeout: 15000 });

		// Welcome heading from HelloView confirms the router landed on `/` and the
		// view component rendered.
		await expect(page.getByRole("heading", { name: /Welcome to Pivotable/ })).toBeVisible();

		// Active mode flags must reflect the URL — guards against a regression where
		// the bootstrap silently flips back to a default that ignores the query param.
		const flags = await page.evaluate(() => ({
			resourceMode: window.__adhocResourceMode,
			minified: window.__adhocMinified,
			dev: window.__adhocDev,
		}));
		expect(flags.resourceMode).toBe(search.includes("cdn") ? "cdn" : "webjars");
		expect(flags.dev).toBe(search.includes("dev"));
		expect(flags.minified).toBe(!search.includes("dev"));

		expect(consoleErrors, `console errors for ${name}`).toEqual([]);
		expect(pageErrors, `page errors for ${name}`).toEqual([]);
	});
}
