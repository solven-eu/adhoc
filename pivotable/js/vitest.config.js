// https://vitest.dev/config/

import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		// https://vitest.dev/config/#include
		include: ["unit-tests/**/*.{test,spec}.js"],
		alias: {
			// similare to `base` in `vite.config.js`
			"@/": "../src/main/resources/static/ui/",
		},
		// Coverage is only produced when `--coverage` is passed on the CLI (e.g. via
		// `npm run coverage_unit`). Default runs stay fast — instrumentation has a real cost.
		coverage: {
			// `v8` provider uses the Node.js built-in Crankshaft coverage — no source
			// transformation needed, and it's bundled via `@vitest/coverage-v8`.
			provider: "v8",
			// Match the source set the tests exercise. Unit tests today cover the pure-JS
			// helpers under `ui/js/`; the rest of the app (components, store glue, etc.) is
			// only exercised via Playwright, and will feed into coverage through Phase 2.
			include: ["src/main/resources/static/ui/js/**/*.js"],
			exclude: [
				"src/main/resources/static/ui/js/**/*.spec.js",
				// Vite-only / third-party bundles we don't own
				"**/node_modules/**",
			],
			// lcov feeds CI; html is a human-readable drill-down.
			// `json` emits `coverage-final.json` (istanbul format) — consumed by
			// `scripts/merge-coverage.mjs` to produce a merged report that combines vitest
			// and Playwright runs.
			reporter: ["text", "html", "lcov", "json"],
			reportsDirectory: "./coverage/unit",
			// `all` means "report on files even if no test touched them" — the whole point of
			// coverage is to see the gaps, not just the hits.
			all: true,
		},
	},
});
