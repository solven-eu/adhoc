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

const STATIC_DIR = join(dirname(fileURLToPath(import.meta.url)), "..", "src", "main", "resources", "static");
const INDEX_HTML = readFileSync(join(STATIC_DIR, "index.html"), "utf8");
const IMPORTMAP_WEBJARS = JSON.parse(readFileSync(join(STATIC_DIR, "ui", "importmap-webjars.json"), "utf8"));
const IMPORTMAP_CDN = JSON.parse(readFileSync(join(STATIC_DIR, "ui", "importmap-cdn.json"), "utf8"));
const IMPORTMAP_WEBJARS_MIN = JSON.parse(readFileSync(join(STATIC_DIR, "ui", "importmap-webjars-min.json"), "utf8"));
const IMPORTMAP_CDN_MIN = JSON.parse(readFileSync(join(STATIC_DIR, "ui", "importmap-cdn-min.json"), "utf8"));

test("index.html reads the override from ?cdn, not ?webjars", () => {
	expect(INDEX_HTML).toMatch(/params\.has\(\s*"cdn"\s*\)/);
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

test("index.html references all four importmap JSONs (cdn × min cross-product)", () => {
	// Default (webjars, MIN) must point at the webjars-min importmap; ?cdn and ?dev select the
	// right variant. Guards against a regression that would route everyone through the CDN,
	// or that would silently flip the default away from minified.
	expect(INDEX_HTML).toContain("/ui/importmap-webjars.json");
	expect(INDEX_HTML).toContain("/ui/importmap-cdn.json");
	expect(INDEX_HTML).toContain("/ui/importmap-webjars-min.json");
	expect(INDEX_HTML).toContain("/ui/importmap-cdn-min.json");
});

test("index.html reads the ?dev flag and derives useMin as its negation (so default = minified)", () => {
	expect(INDEX_HTML).toMatch(/params\.has\(\s*"dev"\s*\)/);
	expect(INDEX_HTML).toMatch(/const\s+useMin\s*=\s*!useDev/);
	expect(INDEX_HTML).toMatch(/window\.__adhocMinified\s*=\s*useMin/);
});

test("local-webjars importmap has every library used by the SPA, with a versioned path", () => {
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
	const importUrls = Object.values(IMPORTMAP_WEBJARS.imports);
	for (const p of REQUIRED_WEBJAR_PATHS) {
		expect(importUrls).toContain(p);
	}
});

test("every local /webjars/ URL in the webjars importmap embeds a version segment — prerequisite for the immutable cache policy", () => {
	// Asserts that every URL in the webjars importmap matches `/webjars/<artifact>/<version>/...`.
	// Version must be a digit-starting segment so any regression to a versionless URL (which
	// Spring Boot would serve with stale content after a WebJar upgrade, given the immutable
	// cache) fails the build.
	const urls = Object.values(IMPORTMAP_WEBJARS.imports);
	expect(urls.length).toBeGreaterThan(0);
	for (const url of urls) {
		expect(url).toMatch(/^\/webjars\/[^/]+\/\d[^/]*\//);
	}
});

test("cdn importmap mirrors the webjars importmap — same keys, same versions", () => {
	// The CDN arm must carry the exact same set of libraries as the WebJars arm, at the same
	// versions — any drift between the two would give the `?cdn` override a different module
	// set than the default, silently masking or introducing bugs depending on which the user
	// happens to load.
	expect(Object.keys(IMPORTMAP_CDN.imports).sort()).toEqual(Object.keys(IMPORTMAP_WEBJARS.imports).sort());
	// Pull the `<version>` segment out of each URL and compare. Three URL shapes are supported:
	//   - WebJars local    : `/webjars/<artifact>/<version>/...`
	//   - jsdelivr WebJars : `https://cdn.jsdelivr.net/webjars/<groupId>/<artifact>/<version>/...`
	//   - jsdelivr npm ESM : `https://cdn.jsdelivr.net/npm/<package>@<version>/...` (used for
	//                        the bundled `lodashEs` CDN variant via `/+esm`).
	const extractVersion = function (url) {
		// Try the `/webjars/.../<version>/` forms first (both local and jsdelivr).
		const webjarsMatch = url.match(/\/webjars\/[^/]+\/(?:[^/]+\/)?(\d[^/]*)\//);
		if (webjarsMatch) {
			return webjarsMatch[1];
		}
		// jsdelivr `/+esm`-style: `<package>@<version>/...`.
		const npmMatch = url.match(/@(\d[^/]*)/);
		if (npmMatch) {
			return npmMatch[1];
		}
		throw new Error(`Cannot extract version from ${url}`);
	};
	for (const key of Object.keys(IMPORTMAP_WEBJARS.imports)) {
		const webjarVersion = extractVersion(IMPORTMAP_WEBJARS.imports[key]);
		const cdnVersion = extractVersion(IMPORTMAP_CDN.imports[key]);
		expect(cdnVersion).toBe(webjarVersion);
	}
});

test("min importmaps carry the same keys and versions as their full-build counterparts", () => {
	// A min importmap must map EXACTLY the same set of specifiers as its full-build sibling —
	// any drift would silently give `?min` callers a different module set than the default.
	expect(Object.keys(IMPORTMAP_WEBJARS_MIN.imports).sort()).toEqual(Object.keys(IMPORTMAP_WEBJARS.imports).sort());
	expect(Object.keys(IMPORTMAP_CDN_MIN.imports).sort()).toEqual(Object.keys(IMPORTMAP_CDN.imports).sort());
	const extract = function (url) {
		const webjarsMatch = url.match(/\/webjars\/[^/]+\/(?:[^/]+\/)?(\d[^/]*)\//);
		if (webjarsMatch) {
			return webjarsMatch[1];
		}
		const npmMatch = url.match(/@(\d[^/]*)/);
		if (npmMatch) {
			return npmMatch[1];
		}
		throw new Error(`Cannot extract version from ${url}`);
	};
	for (const key of Object.keys(IMPORTMAP_WEBJARS.imports)) {
		expect(extract(IMPORTMAP_WEBJARS_MIN.imports[key])).toBe(extract(IMPORTMAP_WEBJARS.imports[key]));
		expect(extract(IMPORTMAP_CDN_MIN.imports[key])).toBe(extract(IMPORTMAP_CDN.imports[key]));
	}
});
