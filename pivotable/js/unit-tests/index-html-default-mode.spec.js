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
	// The real contract is the URL path, not the object key. Paths embed the version so the
	// backend can apply a far-future `immutable` cache header safely — see
	// PivotableWebjarsCachingWebFluxConfigurer / PivotableWebjarsCachingWebmvcConfigurer. If
	// any of these paths goes missing, the corresponding library will 404 at page load.
	const REQUIRED_WEBJAR_PATHS = [
		"/webjars/vue/3.5.32/dist/vue.esm-browser.js",
		"/webjars/vue-router/4.6.3/dist/vue-router.esm-browser.js",
		"/webjars/pinia/3.0.4/dist/pinia.esm-browser.js",
		"/webjars/popperjs__core/2.11.8/dist/esm/index.js",
		"/webjars/slickgrid/5.18.2/dist/esm/index.mjs",
		"/webjars/sortablejs/1.15.7/modular/sortable.esm.js",
		"/webjars/lodash-es/4.17.21/lodash.js",
		"/webjars/mermaid/11.6.0/dist/mermaid.esm.mjs",
		"/webjars/vue-demi/0.14.10/lib/v3/index.mjs",
		"/webjars/vue__devtools-api/6.6.4/lib/esm/index.js",
		"/webjars/bootstrap/5.3.8/js/bootstrap.esm.js",
	];
	for (const p of REQUIRED_WEBJAR_PATHS) {
		expect(INDEX_HTML).toContain(p);
	}
});

test("every local /webjars/ URL embeds a version segment — prerequisite for the immutable cache policy", () => {
	// Extract the `webjars:` arm of each IMPORTS / STYLESHEETS entry and assert the path
	// matches `/webjars/<artifact>/<version>/...`. Version must be a digit-starting segment
	// so any regression to a versionless URL (which Spring Boot would serve with stale
	// content after a WebJar upgrade, given the immutable cache) fails the build.
	const localWebjarsUrls = INDEX_HTML.match(/webjars:\s*"\/webjars\/[^"]+"/g) || [];
	expect(localWebjarsUrls.length).toBeGreaterThan(0);
	for (const url of localWebjarsUrls) {
		expect(url).toMatch(/\/webjars\/[^/]+\/\d[^/]*\//);
	}
});
