// Shared Playwright fixture that wraps every test with Chromium V8 JS-coverage collection.
// Tests import `test` and `expect` from here instead of `@playwright/test` — the extended
// `test` adds an auto-fixture so coverage is gathered without per-test boilerplate.
//
// Collected raw V8 entries are funnelled into a single `CoverageReport` session (one
// directory under `coverage/playwright/`) and the final lcov + html report is emitted in
// Playwright's globalTeardown (`playwright-coverage-teardown.mjs`).
//
// Opt-in: set `PW_COVERAGE=1` in the environment to enable collection. Off by default so
// vanilla `npx playwright test` runs stay as fast as before.

import { test as base, expect } from "@playwright/test";
import { CoverageReport } from "monocart-coverage-reports";
import path from "node:path";
import url from "node:url";

const COVERAGE_ENABLED = process.env.PW_COVERAGE === "1";
const COVERAGE_DIR = path.resolve(path.dirname(url.fileURLToPath(import.meta.url)), "../coverage/playwright");

// Shared report instance. `monocart-coverage-reports` is happy being appended to across the
// whole test run; `generate()` in the globalTeardown flushes everything to disk at once.
// We lazy-init so the constructor doesn't run when coverage is disabled.
let coverageReport = null;
function getReport() {
	if (!coverageReport) {
		coverageReport = new CoverageReport({
			name: "Pivotable Playwright coverage",
			outputDir: COVERAGE_DIR,
			// `v8` input is the raw Chromium V8 coverage format; monocart converts it to
			// istanbul internally so the emitted lcov can be merged with the vitest report.
			// `json` emits `coverage-final.json` (istanbul format) — consumed by the merge
			// script so the vitest and Playwright reports can be combined.
			reports: ["v8", "lcov", "html", "json"],
			// Only report on our own JS — drop webjars, vendored CDN builds, node_modules, etc.
			entryFilter: (entry) => /\/ui\/js\//.test(entry.url),
			sourceFilter: (sourcePath) => /\/ui\/js\//.test(sourcePath),
			// Collapse multiple test hits on the same source file.
			sourceMappedFiles: true,
			cleanCache: false,
		});
	}
	return coverageReport;
}

export const test = base.extend({
	autoCoverage: [
		async ({ page }, use) => {
			if (!COVERAGE_ENABLED) {
				await use();
				return;
			}
			// `resetOnNavigation: false` keeps per-page-load coverage accumulating within one
			// test — otherwise a login + a query-page navigation would only report the latter.
			await page.coverage.startJSCoverage({ resetOnNavigation: false });
			await use();
			const entries = await page.coverage.stopJSCoverage();
			await getReport().add(entries);
		},
		{ auto: true, scope: "test" },
	],
});

export { expect };
