// Pure-Node tests. The auto-fit code reaches for `document.createElement("canvas")` +
// `window.getComputedStyle`, both of which are normally unavailable outside a browser. Rather than
// pull in `jsdom` as a dev-dependency just for this file, we stub the bare-minimum surface area
// the code touches via `vi.stubGlobal`. The stubs let us pin the canvas's measureText to a
// predictable char-width so the assertions are deterministic.

import { expect, test, describe, vi, beforeEach, afterEach } from "vitest";

import {
	stripHtml,
	autoFitColumnWidth,
	applyAutoFitWidth,
	AUTOFIT_MAX_ROWS_PROBED,
	AUTOFIT_PADDING_PX,
	AUTOFIT_MIN_WIDTH_PX,
} from "@/js/adhoc-query-grid-autofit.js";

// ---------------------------------------------------------------------------------------------
// stripHtml — pure, no DOM. Guards the canvas-measure path from counting markup characters
// towards the width.
// ---------------------------------------------------------------------------------------------
describe("stripHtml", () => {
	test("plain text passes through", () => {
		expect(stripHtml("Currency")).toBe("Currency");
	});
	test("strips simple tag wrappers", () => {
		expect(stripHtml("<span>123</span>")).toBe("123");
	});
	test("strips multiple / nested tags", () => {
		expect(stripHtml("<i class='bi bi-copy'></i><span>EUR</span>")).toBe("EUR");
	});
	test("strips attribute-heavy formatter output", () => {
		expect(stripHtml('<span style="color:red" class="x">42.5%</span>')).toBe("42.5%");
	});
	test("null / undefined become empty string", () => {
		expect(stripHtml(null)).toBe("");
		expect(stripHtml(undefined)).toBe("");
	});
	test("non-string inputs are coerced via String()", () => {
		expect(stripHtml(42)).toBe("42");
		expect(stripHtml(0)).toBe("0");
	});
});

// ---------------------------------------------------------------------------------------------
// autoFitColumnWidth — stub canvas + document globals to make the measurement deterministic.
// ---------------------------------------------------------------------------------------------
describe("autoFitColumnWidth", () => {
	const CHAR_PX = 7;

	let savedDocument;
	let savedWindow;
	let canvasCtxFactory = () => ({
		font: "",
		measureText: (text) => ({ width: (text || "").length * CHAR_PX }),
	});

	beforeEach(() => {
		savedDocument = globalThis.document;
		savedWindow = globalThis.window;
		// Stub a minimal `document`: createElement("canvas") returns an object whose getContext
		// gives us the predictable measureText we install per-test; querySelector returns null so
		// the auto-fit code falls back to the default 13px sans-serif font.
		vi.stubGlobal("document", {
			createElement: (tag) => {
				if (tag === "canvas") {
					return { getContext: () => canvasCtxFactory() };
				}
				return {};
			},
			querySelector: () => null,
		});
		vi.stubGlobal("window", { getComputedStyle: () => ({ fontWeight: "400", fontSize: "13px", fontFamily: "sans-serif" }) });
	});

	afterEach(() => {
		vi.unstubAllGlobals();
		globalThis.document = savedDocument;
		globalThis.window = savedWindow;
	});

	test("returns 0 when canvas.getContext returns null", () => {
		canvasCtxFactory = () => null;
		const grid = {};
		const dataView = { getLength: () => 0, getItem: () => null };
		expect(autoFitColumnWidth(grid, dataView, { name: "x", field: "x" }, 0)).toBe(0);
		// Reset for subsequent tests in this describe.
		canvasCtxFactory = () => ({ font: "", measureText: (text) => ({ width: (text || "").length * CHAR_PX }) });
	});

	test("returns minimum width on a short header + no rows", () => {
		const grid = {};
		const dataView = { getLength: () => 0, getItem: () => null };
		// Header "x" = 1 char × 7px + padding 16 = 23 → bumped up to AUTOFIT_MIN_WIDTH_PX (40).
		const w = autoFitColumnWidth(grid, dataView, { name: "x", field: "x" }, 0);
		expect(w).toBe(AUTOFIT_MIN_WIDTH_PX);
	});

	test("widest row wins over the header", () => {
		const grid = {};
		const items = [{ v: "tiny" }, { v: "a much longer cell value here" }, { v: "mid" }];
		const dataView = { getLength: () => items.length, getItem: (i) => items[i] };
		// Header "v" → 7px. Widest cell "a much longer cell value here" = 29 chars × 7 = 203px.
		// + AUTOFIT_PADDING_PX 16 = 219px.
		const w = autoFitColumnWidth(grid, dataView, { name: "v", field: "v" }, 0);
		expect(w).toBe(29 * CHAR_PX + AUTOFIT_PADDING_PX);
	});

	test("formatter output is what gets measured (not the raw field value)", () => {
		const grid = {};
		const items = [{ v: 42 }];
		const dataView = { getLength: () => 1, getItem: (i) => items[i] };
		// Raw `42` would be 2 chars; the formatter expands it to a wider string with HTML wrappers.
		// stripHtml removes the wrappers, so the measured text is "value=42 %" → 10 chars.
		const formatter = () => '<span class="foo">value=42 %</span>';
		const w = autoFitColumnWidth(grid, dataView, { name: "v", field: "v", formatter }, 0);
		expect(w).toBe(10 * CHAR_PX + AUTOFIT_PADDING_PX);
	});

	test("rows past AUTOFIT_MAX_ROWS_PROBED are ignored — bounded scan", () => {
		const grid = {};
		const cap = AUTOFIT_MAX_ROWS_PROBED;
		// All sampled rows are short; the one wide outlier sits PAST the cap and must be ignored.
		const items = Array.from({ length: cap + 100 }, (_, i) => ({ v: i === cap + 50 ? "this-is-very-long-and-should-be-ignored" : "x" }));
		const dataView = { getLength: () => items.length, getItem: (i) => items[i] };
		const w = autoFitColumnWidth(grid, dataView, { name: "v", field: "v" }, 0);
		// All sampled values are "x" (1 char); header "v" (1 char). Width = 1*7 + 16 = 23 → floor to 40.
		expect(w).toBe(AUTOFIT_MIN_WIDTH_PX);
	});

	test("formatter that throws is gracefully handled — falls back to the raw value", () => {
		const grid = {};
		const items = [{ v: "raw-value-here" }];
		const dataView = { getLength: () => 1, getItem: (i) => items[i] };
		const formatter = () => {
			throw new Error("boom");
		};
		// Falls back to item[field] = "raw-value-here" = 14 chars.
		const w = autoFitColumnWidth(grid, dataView, { name: "v", field: "v", formatter }, 0);
		expect(w).toBe(14 * CHAR_PX + AUTOFIT_PADDING_PX);
	});

	test("null cells are skipped, not measured as 'null'", () => {
		const grid = {};
		const items = [{ v: null }, { v: undefined }, { v: "abc" }];
		const dataView = { getLength: () => items.length, getItem: (i) => items[i] };
		const w = autoFitColumnWidth(grid, dataView, { name: "v", field: "v" }, 0);
		// Only "abc" gets measured: 3 chars × 7 + 16 = 37 → floor to 40.
		expect(w).toBe(AUTOFIT_MIN_WIDTH_PX);
	});
});

// ---------------------------------------------------------------------------------------------
// applyAutoFitWidth — the mutation half of the auto-fit pipeline. Tests cover idempotency,
// out-of-range column-index guards, the in-place mutation contract, and the setColumns dispatch.
// ---------------------------------------------------------------------------------------------
describe("applyAutoFitWidth", () => {
	// Build a grid mock with the methods applyAutoFitWidth touches. By default both the fast
	// (`applyColumnWidths`) path and the fallback (`setColumns`) path are available; individual
	// tests override `applyColumnWidths` to undefined to exercise the fallback.
	const makeGrid = (cols, overrides = {}) => {
		const applyCalls = [];
		const setColumnsCalls = [];
		const notifyCalls = [];
		return {
			getColumns: () => cols,
			applyColumnWidths: () => applyCalls.push(true),
			setColumns: (newCols) => setColumnsCalls.push(newCols),
			getContainerNode: () => null,
			onColumnsResized: { notify: () => notifyCalls.push(true) },
			_applyCalls: applyCalls,
			_setColumnsCalls: setColumnsCalls,
			_notifyCalls: notifyCalls,
			...overrides,
		};
	};

	test("idempotent: no work when the column already sits at the target width", () => {
		const cols = [
			{ id: "a", width: 100 },
			{ id: "b", width: 50 },
		];
		const grid = makeGrid(cols);
		const result = applyAutoFitWidth(grid, 0, 100);
		expect(result).toBe(false);
		expect(grid._applyCalls).toHaveLength(0);
		expect(grid._setColumnsCalls).toHaveLength(0);
		expect(grid._notifyCalls).toHaveLength(0);
		expect(cols[0].width).toBe(100);
	});

	test("fast path: mutates the target column in place + calls applyColumnWidths (NOT setColumns)", () => {
		const cols = [
			{ id: "a", width: 100 },
			{ id: "b", width: 50 },
		];
		const grid = makeGrid(cols);
		const result = applyAutoFitWidth(grid, 1, 250);
		expect(result).toBe(true);
		expect(cols[1].width).toBe(250);
		// First column untouched — confirms scroll-mode preservation contract.
		expect(cols[0].width).toBe(100);
		// Fast path: applyColumnWidths is called, setColumns is NOT. setColumns re-renders
		// every row and was the source of the 1-3 second dblclick lag.
		expect(grid._applyCalls).toHaveLength(1);
		expect(grid._setColumnsCalls).toHaveLength(0);
		// Notify must fire so the per-grid columnWidthMemo watcher in `adhoc-query-grid.js` picks
		// up the manual auto-fit and persists it across resyncs.
		expect(grid._notifyCalls).toHaveLength(1);
	});

	test("fallback path: setColumns when applyColumnWidths is unavailable", () => {
		const cols = [{ id: "a", width: 100 }];
		const grid = makeGrid(cols, { applyColumnWidths: undefined });
		const result = applyAutoFitWidth(grid, 0, 200);
		expect(result).toBe(true);
		expect(cols[0].width).toBe(200);
		expect(grid._setColumnsCalls).toHaveLength(1);
	});

	test("returns false when grid is null or missing getColumns", () => {
		expect(applyAutoFitWidth(null, 0, 100)).toBe(false);
		expect(applyAutoFitWidth({}, 0, 100)).toBe(false);
	});

	test("returns false when colIdx is out of range", () => {
		const cols = [{ id: "a", width: 100 }];
		const grid = makeGrid(cols);
		expect(applyAutoFitWidth(grid, -1, 200)).toBe(false);
		expect(applyAutoFitWidth(grid, 5, 200)).toBe(false);
		expect(grid._applyCalls).toHaveLength(0);
		expect(grid._setColumnsCalls).toHaveLength(0);
	});
});
