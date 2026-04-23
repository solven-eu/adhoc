// Playwright globalTeardown — flushes the accumulated V8 coverage to lcov + html once the
// whole test run has finished. Invoked by `playwright.config.mjs` only when PW_COVERAGE=1.
//
// The report is built incrementally by the per-test fixture in `_coverage-fixture.mjs`;
// this teardown just calls `.generate()` to write the final artifacts to
// `coverage/playwright/`.

import { CoverageReport } from "monocart-coverage-reports";
import path from "node:path";
import url from "node:url";

export default async function globalTeardown() {
	if (process.env.PW_COVERAGE !== "1") return;
	const outputDir = path.resolve(path.dirname(url.fileURLToPath(import.meta.url)), "../coverage/playwright");
	// Re-open the session at the same outputDir; monocart persists intermediate state so the
	// generate() call here picks up everything the per-test fixture wrote during the run.
	const report = new CoverageReport({
		name: "Pivotable Playwright coverage",
		outputDir,
		reports: ["v8", "lcov", "html", "json"],
		cleanCache: false,
	});
	await report.generate();
	// eslint-disable-next-line no-console
	console.log("Playwright coverage report written to", outputDir);
}
