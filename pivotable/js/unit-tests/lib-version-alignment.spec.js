// @ts-check
//
// Cross-layer version-alignment gate for libraries used by the Pivotable SPA.
//
// Each library lives in MULTIPLE files, and a partial bump (touching only one) silently produces
// a runtime ≠ types mismatch:
//   1. `pivotable/js/pom.xml`               — WebJar version served from `/webjars/<artifact>/<v>/...`
//   2. `importmap-webjars{,-min}.json`      — `/webjars/<artifact>/<v>/...` URLs the browser resolves
//   3. `importmap-cdn{,-min}.json`          — `https://cdn.jsdelivr.net/webjars/.../<v>/...` mirrors
//   4. `vite.config.js`                     — `IMPORTMAP_ALIASES` table feeding Vite's dev server
//   5. `pivotable/js/package.json`          — devDep at the same version, purely so `@types/*` align
//
// Previously a Renovate PR like #778 (bumped only `pom.xml`'s lodash-es webjar) could land and
// leave the importmaps + vite.config.js stuck on the old version — fixed at runtime only when
// someone manually noticed. This test parses every layer's actual content and asserts the
// version segments are identical for each tracked library, so any future partial bump fails CI.
//
// The Renovate config (`renovate.json`) ALSO groups every Pivotable SPA library into a single
// per-library PR so Renovate proposes the bumps together — this test is the safety net for the
// case where a human still lands a partial bump.

import { describe, test, expect } from "vitest";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const JS_ROOT = path.resolve(__dirname, "..");

// One row per library that's pinned across multiple layers. Each row says:
//   - webjarArtifactId : the `<artifactId>` in pom.xml under `org.webjars.npm` (or `org.webjars`)
//   - webjarUrlSegment : what shows up in importmap URL paths
//                        (often equal to artifactId, but sometimes `popperjs__core` ≠ `@popperjs/core`)
//   - cdnUrlSegment    : what shows up in the CDN form (sometimes different — e.g. `lodash-es@4.18.1`
//                        with a `@version` separator on jsDelivr vs `lodash-es/4.18.1` on /webjars/)
//   - importmapKey     : the bare-specifier key in the four importmap JSONs
//   - npmName          : the matching devDep name in package.json (`null` if not installed)
//   - npmAlias         : when the devDep is aliased (e.g. `npm:lodash@x.y.z`), the inner pkg name
const TRACKED = [
	{
		webjarArtifactId: "lodash-es",
		webjarUrlSegment: "lodash-es",
		cdnUrlSegment: "lodash-es",
		importmapKey: "lodashEs",
		npmName: "lodashEs",
		npmAlias: "lodash",
	},
	{
		webjarArtifactId: "mermaid",
		webjarUrlSegment: "mermaid",
		cdnUrlSegment: "mermaid",
		importmapKey: "mermaid",
		npmName: "mermaid",
		npmAlias: null,
	},
	{
		webjarArtifactId: "slickgrid",
		webjarUrlSegment: "slickgrid",
		cdnUrlSegment: "slickgrid",
		importmapKey: "slickgrid",
		npmName: "slickgrid",
		npmAlias: null,
	},
	{
		webjarArtifactId: "sortablejs",
		webjarUrlSegment: "sortablejs",
		cdnUrlSegment: "sortablejs",
		importmapKey: "sortablejs",
		npmName: "sortablejs",
		npmAlias: null,
	},
	{
		webjarArtifactId: "vue",
		webjarUrlSegment: "vue",
		cdnUrlSegment: "vue",
		importmapKey: "vue",
		npmName: "vue",
		npmAlias: null,
	},
	{
		webjarArtifactId: "vue-router",
		webjarUrlSegment: "vue-router",
		cdnUrlSegment: "vue-router",
		importmapKey: "vue-router",
		npmName: "vue-router",
		npmAlias: null,
	},
	{
		webjarArtifactId: "pinia",
		webjarUrlSegment: "pinia",
		cdnUrlSegment: "pinia",
		importmapKey: "pinia",
		npmName: "pinia",
		npmAlias: null,
	},
	{
		webjarArtifactId: "bootstrap",
		webjarUrlSegment: "bootstrap",
		cdnUrlSegment: "bootstrap",
		importmapKey: "bootstrap",
		npmName: "bootstrap",
		npmAlias: null,
	},
];

/**
 * @param {string} relPath
 * @returns {string}
 */
function readText(relPath) {
	return fs.readFileSync(path.join(JS_ROOT, relPath), "utf-8");
}

/**
 * Extract the `<version>` value for a given `<artifactId>` block from `pivotable/js/pom.xml`.
 * Regex-based (no XML parser dep) — pom.xml is generated with a stable enough shape for this to
 * be robust as long as `<artifactId>X</artifactId>` and `<version>Y</version>` appear in that
 * order inside one `<dependency>` element.
 *
 * @param {string} artifactId
 * @returns {string | null}
 */
function pomVersion(artifactId) {
	const pom = readText("pom.xml");
	const pattern = new RegExp(`<artifactId>${artifactId}</artifactId>\\s*<version>([^<]+)</version>`);
	const match = pom.match(pattern);
	return match ? match[1] : null;
}

/**
 * @param {string} relPath
 * @param {string} importmapKey
 * @returns {string | null} URL associated with the key, or null when absent
 */
function importmapUrl(relPath, importmapKey) {
	const obj = JSON.parse(readText(relPath));
	return (obj.imports && obj.imports[importmapKey]) || null;
}

/**
 * Pull a version segment out of a webjar-style URL. Accepts both forms:
 *   - `/webjars/<artifact>/<version>/...`               (importmap-webjars*.json)
 *   - `https://cdn.jsdelivr.net/webjars/.../<v>/...`    (importmap-cdn*.json, same path shape)
 *   - `https://cdn.jsdelivr.net/npm/<artifact>@<v>/...` (jsDelivr's `@version` separator)
 *
 * @param {string} url
 * @param {string} artifactSegment
 * @returns {string | null}
 */
function extractVersionFromUrl(url, artifactSegment) {
	// Either `<segment>/<version>/` (webjars-style) OR `<segment>@<version>/` (jsdelivr-npm-style).
	const escaped = artifactSegment.replace(/[/\-\\^$*+?.()|[\]{}]/g, "\\$&");
	const pattern = new RegExp(`${escaped}[/@]([^/]+)/`);
	const match = url.match(pattern);
	return match ? match[1] : null;
}

/**
 * Pull the npm devDep version for a given package name from `package.json`. Handles the
 * `npm:<alias>@<version>` aliasing form (we use `lodashEs: npm:lodash@4.18.1`).
 *
 * @param {string} pkgName
 * @returns {string | null}
 */
function packageJsonVersion(pkgName) {
	const obj = JSON.parse(readText("package.json"));
	const spec = (obj.devDependencies && obj.devDependencies[pkgName]) || (obj.dependencies && obj.dependencies[pkgName]) || null;
	if (!spec) return null;
	// `npm:lodash@4.18.1` → version is what's after the last `@`.
	if (spec.startsWith("npm:")) {
		const m = spec.match(/@([^@]+)$/);
		return m ? m[1] : null;
	}
	// Plain version string.
	return spec.replace(/^[~^]/, "");
}

const VITE_CONFIG = readText("vite.config.js");

describe("library version alignment across SPA loading layers", () => {
	for (const lib of TRACKED) {
		test(`${lib.webjarArtifactId} — pom.xml is the source of truth, every other layer matches`, () => {
			const expectedVersion = pomVersion(lib.webjarArtifactId);
			expect(expectedVersion, `Couldn't extract pom.xml version for ${lib.webjarArtifactId}`).toBeTruthy();

			// (2) webjars + min — local-loaded path uses `/webjars/<segment>/<v>/...`
			for (const file of ["src/main/resources/static/ui/importmap-webjars.json", "src/main/resources/static/ui/importmap-webjars-min.json"]) {
				const url = importmapUrl(file, lib.importmapKey);
				expect(url, `${file} missing importmap entry for "${lib.importmapKey}"`).toBeTruthy();
				if (url) {
					const v = extractVersionFromUrl(url, lib.webjarUrlSegment);
					expect(v, `Could not read version from ${file} URL ${url}`).toBe(expectedVersion);
				}
			}

			// (3) cdn + min — jsDelivr URL, may use `@version` or `/version/` depending on the path shape.
			for (const file of ["src/main/resources/static/ui/importmap-cdn.json", "src/main/resources/static/ui/importmap-cdn-min.json"]) {
				const url = importmapUrl(file, lib.importmapKey);
				expect(url, `${file} missing importmap entry for "${lib.importmapKey}"`).toBeTruthy();
				if (url) {
					const v = extractVersionFromUrl(url, lib.cdnUrlSegment);
					expect(v, `Could not read version from ${file} URL ${url}`).toBe(expectedVersion);
				}
			}

			// (4) vite.config.js — IMPORTMAP_ALIASES table. Search for the importmap key on a line and pull
			// the version segment from its URL.
			const viteLinePattern = new RegExp(`"?${lib.importmapKey}"?\\s*:\\s*"([^"]+)"`);
			const viteMatch = VITE_CONFIG.match(viteLinePattern);
			expect(viteMatch, `vite.config.js missing IMPORTMAP_ALIASES entry for "${lib.importmapKey}"`).toBeTruthy();
			if (viteMatch) {
				const v = extractVersionFromUrl(viteMatch[1], lib.webjarUrlSegment);
				expect(v, `Could not read version from vite.config.js URL ${viteMatch[1]}`).toBe(expectedVersion);
			}

			// (5) package.json devDep — when the library has a matching npm entry. The alias form
			// (`lodashEs: npm:lodash@4.18.1`) is unpacked; plain entries pass through.
			if (lib.npmName) {
				const npmVersion = packageJsonVersion(lib.npmName);
				expect(npmVersion, `package.json missing devDep/dep entry for "${lib.npmName}"`).toBeTruthy();
				expect(npmVersion).toBe(expectedVersion);
			}
		});
	}
});
