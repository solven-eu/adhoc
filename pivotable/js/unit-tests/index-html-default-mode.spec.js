import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { expect, test } from "vitest";

// Regression test for the static-resource source default in `index.html`. The bootstrap script
// at the top of <head> picks between local WebJars (served by Spring Boot at `/webjars/*`) and
// jsdelivr's WebJars mirror (CDN) based on the `?cdn` query parameter. The DEFAULT (no query
// param) must stay on WebJars because:
//   1. it keeps the SPA air-gap-safe out of the box (no outbound CDN dependency);
//   2. it matches the security contract documented in SECURITY.MD § Pivotable index.html.
// An accidental flip of the default (e.g. someone re-introducing `has("webjars")`) would
// silently push every user onto the CDN at next page load — this test guards against that.

const INDEX_HTML = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "src", "main", "resources", "static", "index.html"), "utf8");

test("index.html reads the override from ?cdn, not ?webjars", () => {
	expect(INDEX_HTML).toMatch(/new\s+URLSearchParams\(\s*location\.search\s*\)\.has\(\s*"cdn"\s*\)/);
	// The old default-to-CDN form read the override from `?webjars`. Any regression to that
	// wording flips the default back to CDN — fail loudly if it reappears.
	expect(INDEX_HTML).not.toMatch(/\.has\(\s*"webjars"\s*\)/);
});

test("index.html derives useWebjars as the NEGATION of useCdn (so no-query-param = webjars)", () => {
	expect(INDEX_HTML).toMatch(/const\s+useWebjars\s*=\s*!useCdn/);
});

test("index.html exposes the active mode on window.__adhocResourceMode for DevTools inspection", () => {
	expect(INDEX_HTML).toMatch(/window\.__adhocResourceMode\s*=\s*useWebjars\s*\?\s*"webjars"\s*:\s*"cdn"/);
});

test("local-webjars path for every library used by the SPA is present", () => {
	// The real contract is the URL path, not the object key: these are the versionless
	// paths the browser actually fetches when `useWebjars` is true. If any goes missing,
	// the corresponding library will 404 at page load in the default mode.
	const REQUIRED_WEBJAR_PATHS = [
		"/webjars/vue/dist/vue.esm-browser.js",
		"/webjars/vue-router/dist/vue-router.esm-browser.js",
		"/webjars/pinia/dist/pinia.esm-browser.js",
		"/webjars/popperjs__core/dist/esm/index.js",
		"/webjars/slickgrid/dist/esm/index.mjs",
		"/webjars/sortablejs/modular/sortable.esm.js",
		"/webjars/lodash-es/lodash.js",
		"/webjars/mermaid/dist/mermaid.esm.mjs",
		"/webjars/vue-demi/lib/v3/index.mjs",
		"/webjars/vue__devtools-api/lib/esm/index.js",
		"/webjars/bootstrap/js/bootstrap.esm.js",
	];
	for (const p of REQUIRED_WEBJAR_PATHS) {
		expect(INDEX_HTML).toContain(p);
	}
});

test("local webjars URLs are versionless (webjars-locator-core resolves the version at runtime)", () => {
	// `/webjars/<artifact>/...` — no numeric version segment right after the artifact name.
	// This is what lets us upgrade a library by bumping pom.xml alone, without editing URLs.
	const localWebjarsUrls = INDEX_HTML.match(/webjars:\s*"\/webjars\/[^"]+"/g) || [];
	expect(localWebjarsUrls.length).toBeGreaterThan(0);
	for (const url of localWebjarsUrls) {
		// Reject `/webjars/foo/1.2.3/...` — the version should NOT appear between artifact
		// and path. Matches `/webjars/<name>/<digit-starting-segment>/...`.
		expect(url).not.toMatch(/\/webjars\/[^/]+\/\d/);
	}
});
