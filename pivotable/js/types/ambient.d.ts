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

declare module "bootstrap";
declare module "bootstrap/*";
declare module "mermaid";
declare module "slickgrid";
declare module "sortablejs";
declare module "pinia";
declare module "vue-router";

// `lodashEs` is a package.json alias to `lodash` (the original CommonJS build). lodash 4.x ships
// per-function modules as `lodash/<name>.js` but no TypeScript declarations.
declare module "lodashEs";
declare module "lodashEs/*";

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
