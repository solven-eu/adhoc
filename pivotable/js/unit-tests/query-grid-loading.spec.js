import { expect, test } from "vitest";

import { isLoading, loadingMessage, loadingPercent } from "@/js/adhoc-query-grid-loading.js";

function view(loading) {
	return { loading };
}

test("isLoading: false when tabularView is null / undefined / no loading field", () => {
	expect(isLoading(null)).toBe(false);
	expect(isLoading(undefined)).toBe(false);
	expect(isLoading({})).toBe(false);
});

test("isLoading: false when all flags are false", () => {
	expect(isLoading(view({ sending: false, executing: false }))).toBe(false);
});

test("isLoading: true as soon as one boolean flag is truthy", () => {
	expect(isLoading(view({ sending: false, executing: true }))).toBe(true);
});

test("isLoading: non-boolean fields (e.g. latestFetched) are ignored, not treated as 'loading'", () => {
	// `latestFetched` is set by the executor as a Date object; without the boolean guard it
	// would keep the progress bar pinned at "loading" forever after the first query.
	expect(isLoading(view({ latestFetched: new Date() }))).toBe(false);
});

test("loadingPercent: 100 when idle", () => {
	expect(loadingPercent(view({}))).toBe(100);
	expect(loadingPercent(view({ sending: false }))).toBe(100);
});

test("loadingPercent: advances monotonically through the stage pipeline", () => {
	expect(loadingPercent(view({ sending: true }))).toBe(10);
	expect(loadingPercent(view({ executing: true }))).toBe(20);
	expect(loadingPercent(view({ downloading: true }))).toBe(75);
	expect(loadingPercent(view({ preparingGrid: true }))).toBe(85);
	expect(loadingPercent(view({ rendering: true }))).toBe(90);
});

test("loadingPercent: unknown-but-truthy flag falls back to 95 (transitional)", () => {
	expect(loadingPercent(view({ sorting: true }))).toBe(95);
});

test("loadingMessage: 'Loaded' when idle", () => {
	expect(loadingMessage(view({}))).toBe("Loaded");
});

test("loadingMessage: stage-specific labels match the pipeline", () => {
	expect(loadingMessage(view({ sending: true }))).toBe("Sending the query");
	expect(loadingMessage(view({ downloading: true }))).toBe("Downloading the result");
	expect(loadingMessage(view({ preparingGrid: true }))).toBe("Preparing the grid");
	expect(loadingMessage(view({ rendering: true }))).toBe("Rendering the grid");
});

test("loadingMessage: executing phase distinguishes fetching vs. sleeping vs. unknown", () => {
	expect(loadingMessage(view({ executing: true, fetching: true }))).toBe("Executing the query (fetching)");
	expect(loadingMessage(view({ executing: true, sleeping: true }))).toBe("Executing the query (sleeping)");
	expect(loadingMessage(view({ executing: true }))).toBe("Executing the query (?)");
});

test("loadingMessage: fallback when a truthy flag isn't one of the known stages", () => {
	expect(loadingMessage(view({ sorting: true }))).toBe("Unclear but not done yet");
});
