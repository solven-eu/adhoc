#!/usr/bin/env node
// Merge per-source coverage reports (vitest unit + Playwright e2e) into a third report
// `coverage/merged/`. Each source stays untouched under `coverage/unit/` and
// `coverage/playwright/` so you can still drill into them individually; the merged view
// is for "overall coverage of pivotable/js" questions.
//
// Inputs:
//   - coverage/unit/coverage-final.json        (vitest v8 provider)
//   - coverage/playwright/coverage-final.json  (monocart-coverage-reports, json reporter)
//
// Outputs (under coverage/merged/):
//   - coverage-final.json  (istanbul format, suitable for further tooling)
//   - lcov.info            (CI-friendly, consumed by Codecov / SonarCloud / Coveralls)
//   - html/                (human drill-down: open coverage/merged/index.html)
//
// Idempotent — you can re-run after either source emits a new report and the merged output
// is rebuilt from scratch each time.

import fs from "node:fs";
import path from "node:path";
import url from "node:url";

import libCoverage from "istanbul-lib-coverage";
import libReport from "istanbul-lib-report";
import reports from "istanbul-reports";

const root = path.resolve(path.dirname(url.fileURLToPath(import.meta.url)), "..");
const sources = [
	{ name: "vitest unit", path: path.join(root, "coverage/unit/coverage-final.json") },
	{ name: "playwright e2e", path: path.join(root, "coverage/playwright/coverage-final.json") },
];
const mergedDir = path.join(root, "coverage/merged");

function loadIfExists(source) {
	if (!fs.existsSync(source.path)) {
		console.warn(`merge-coverage: ${source.name} input missing at ${source.path} — skipped`);
		return null;
	}
	try {
		return JSON.parse(fs.readFileSync(source.path, "utf8"));
	} catch (e) {
		console.warn(`merge-coverage: ${source.name} input at ${source.path} could not be parsed: ${e.message}`);
		return null;
	}
}

const coverageMap = libCoverage.createCoverageMap({});
let loaded = 0;
for (const src of sources) {
	const data = loadIfExists(src);
	if (data) {
		coverageMap.merge(data);
		loaded++;
		console.log(`merge-coverage: merged ${src.name} (${Object.keys(data).length} files)`);
	}
}

if (loaded === 0) {
	console.error("merge-coverage: no inputs found — run `npm run coverage_unit` and/or `npm run coverage_pw` first.");
	process.exit(1);
}

fs.mkdirSync(mergedDir, { recursive: true });

const context = libReport.createContext({
	dir: mergedDir,
	coverageMap,
	defaultSummarizer: "nested",
});

// coverage-final.json is the istanbul summary that downstream tools (nyc, Codecov) can pick
// up; lcov is CI-friendly; html is the human drill-down.
reports.create("json", { file: "coverage-final.json" }).execute(context);
reports.create("lcov").execute(context);
reports.create("html", { subdir: "html" }).execute(context);
reports.create("text-summary").execute(context);

console.log(`merge-coverage: wrote merged report to ${mergedDir}`);
