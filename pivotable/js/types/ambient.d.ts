// Ambient module declarations for libraries that are loaded by the SPA via the browser importmap
// (mapped to WebJar / CDN URLs at runtime) and therefore have no entry in `node_modules` for
// TypeScript to discover. Each declaration here is intentionally permissive — its only purpose
// is to keep `// @ts-check` files compilable. If/when we want real type checking against any of
// these modules, install the corresponding `@types/*` package or replace the wildcard with a
// hand-written `declare module '...' { export const X: ...; }` block.
//
// The bare `declare module 'name';` form types the module as `any` AND allows arbitrary
// named/default imports — what we need for `import { mapState } from 'pinia'`,
// `import Sortable from 'sortablejs'`, etc.

// `bootstrap` ships its own .d.ts as of 5.x.
declare module "bootstrap/*";
// `mermaid` ships its own .d.ts (installed at the same version pinned in the importmap).
// `slickgrid` ships its own .d.ts (installed at the same version pinned in the importmap).
// `sortablejs` types come from `@types/sortablejs`.
// `pinia` ships its own .d.ts (installed at the same version pinned in the importmap). Strict typing
// is enabled now that the three stores declare their state via JSDoc `@typedef`; remaining errors
// at call sites with `mapState` getter functions whose `this` is the Vue Options-API component need
// a `/** @type {any} */ (this)` cast (or a migration to `storeToRefs` inside `setup()`).
// `vue-router` ships its own .d.ts (installed at the same version pinned in the importmap).

// `lodashEs` is a package.json alias to `lodash` (the original CommonJS build) — `@types/lodash`
// provides types for the underlying package but TypeScript looks them up by the IMPORT specifier
// (`lodashEs`), so we re-export the `lodash` types under the aliased name. Per-function imports
// (`lodashEs/debounce.js`) resolve to `lodash/<fn>` typings via the wildcard re-export.
declare module "lodashEs" {
	import lodash = require("lodash");
	export = lodash;
}
declare module "lodashEs/*" {
	const fn: any;
	export default fn;
}

// SPA loads `export-to-csv` directly from a jsDelivr URL — TypeScript cannot follow that, so we
// declare the URL as a module.
declare module "https://cdn.jsdelivr.net/npm/export-to-csv@*";
// Catch-all for any other absolute URL imports the SPA may grow over time.
declare module "https://*";

// `String.prototype.hashCode` is installed by `adhoc-query-helper.js` as a project-wide extension
// — declare it globally so call sites under `// @ts-check` can use it.
interface String {
	hashCode(): number;
}

// Global `Sortable` placed on `window` by the sortablejs WebJar — accessed from
// `adhoc-query-grid-helper.js` to wire up column drag. The actual import is also available, but
// the legacy code reads it off `window` in a few spots.
//
// `clickAddMeasure` is a global bridge stamped on `window` by `adhoc-measures-dag.js` so a
// DOM-rendered anchor can fire a Vue handler without needing event-delegation plumbing.
interface Window {
	Sortable?: any;
	clickAddMeasure?: (...args: any[]) => any;
	// Diagnostic flags set by `index.html` at bootstrap time and read by e2e tests to verify
	// which resource-loading mode the page is using (webjars / cdn / dev).
	__adhocResourceMode?: string;
	__adhocMinified?: boolean;
	__adhocDev?: boolean;
}
