// @ts-check
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
	applyInitialScrollAutofit,
	AUTOFIT_MAX_ROWS_PROBED,
	AUTOFIT_PADDING_PX,
	AUTOFIT_MIN_WIDTH_PX,
	AUTOFIT_HEADER_CHROME_PX,
	SCROLL_MODE_AUTOFIT_CAP_PX,
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

	// Regression / CodeQL `js/incomplete-multi-character-sanitization`. The flagged concern is
	// that a single-pass `replace(/<[^>]+>/g, "")` can leave residue when an attacker (or a
	// pathological cell formatter) emits an angle bracket that the regex re-assembles into a
	// fresh tag on the next read. The fixed implementation re-runs the regex until the input
	// stabilises (the "iterative strip" pattern recommended by CodeQL).
	//
	// The contract we pin: (a) the loop is idempotent (running it twice gives the same result
	// as once), (b) no `<` survives in the output regardless of input nesting, and (c) clean
	// inputs are unchanged.
	test("CodeQL: iterative strip leaves no `<` in the output", () => {
		// Nested-tag bypass attempts: even if the regex's greediness leaves partial fragments,
		// the loop keeps stripping until no more `<...>` patterns are present.
		const nested = "<scr<script>ipt>alert(1)</scr</script>ipt>";
		expect(stripHtml(nested)).not.toContain("<");
		const doubleNested = "<<span></span>span>hello</<span></span>span>";
		expect(stripHtml(doubleNested)).not.toContain("<");

		// Clean inputs round-trip without surprise: regular tag wrappers strip to their text.
		expect(stripHtml("<span>OK</span>")).toBe("OK");
		expect(stripHtml("<i class='bi'></i><b>bold</b>")).toBe("bold");
	});

	test("CodeQL: stripHtml is idempotent (running it twice == running it once)", () => {
		const samples = ["plain", "<span>x</span>", "<a><b><c>nested</c></b></a>", "<scr<script>ipt>x</scr</script>ipt>", "<<><>><><<>>", ""];
		for (const s of samples) {
			expect(stripHtml(stripHtml(s))).toBe(stripHtml(s));
		}
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

	test("short header + no rows still reserves header text + chrome (no shrunken column on empty load)", () => {
		const grid = {};
		const dataView = { getLength: () => 0, getItem: () => null };
		// Header DOM lookup returns null in the stub, but `autoFitColumnWidth` falls back to a
		// canvas-text measurement of the header NAME + adds the chrome budget. This is the contract
		// that makes initial-scroll-mode load match toggle behaviour: without the canvas-header
		// fallback, columns measured during requestAnimationFrame (before DOM layout) would lose
		// the 42 px chrome budget and end up much narrower than after a fit→scroll toggle.
		// Header "x" = 1 char × 7 + AUTOFIT_HEADER_CHROME_PX (42) = 49 px; cells 0 → max 49;
		// result = max(MIN, 49 + AUTOFIT_PADDING_PX) = 51.
		const w = autoFitColumnWidth(grid, dataView, { name: "x", field: "x" }, 0);
		expect(w).toBe(1 * CHAR_PX + AUTOFIT_HEADER_CHROME_PX + AUTOFIT_PADDING_PX);
		// And the floor still applies — we never under-shoot AUTOFIT_MIN_WIDTH_PX.
		expect(w).toBeGreaterThanOrEqual(AUTOFIT_MIN_WIDTH_PX);
	});

	test("widest row wins over the header", () => {
		const grid = {};
		const items = [{ v: "tiny" }, { v: "a much longer cell value here" }, { v: "mid" }];
		const dataView = { getLength: () => items.length, getItem: (i) => items[i] };
		// Header canvas-fallback for "v" = 7 + 42 = 49 px. Cells go through the canvas path: widest
		// cell "a much longer cell value here" = 29 chars × 7 = 203 px + 8 px cell padding = 211 px.
		// max(49, 211) = 211; result = 211 + AUTOFIT_PADDING_PX (2) = 213 px.
		const w = autoFitColumnWidth(grid, dataView, { name: "v", field: "v" }, 0);
		expect(w).toBe(29 * CHAR_PX + 8 + AUTOFIT_PADDING_PX);
	});

	test("formatter output is what gets measured (not the raw field value)", () => {
		const grid = {};
		const items = [{ v: 42 }];
		const dataView = { getLength: () => 1, getItem: (i) => items[i] };
		// Raw `42` would be 2 chars; the formatter expands it to a wider string with HTML wrappers.
		// stripHtml removes the wrappers, so the measured text is "value=42 %" → 10 chars.
		// Plus 8 px cell padding + AUTOFIT_PADDING_PX (2) = 80 px.
		const formatter = () => '<span class="foo">value=42 %</span>';
		const w = autoFitColumnWidth(grid, dataView, { name: "v", field: "v", formatter }, 0);
		expect(w).toBe(10 * CHAR_PX + 8 + AUTOFIT_PADDING_PX);
	});

	test("rows past AUTOFIT_MAX_ROWS_PROBED are ignored — bounded scan", () => {
		const grid = {};
		const cap = AUTOFIT_MAX_ROWS_PROBED;
		// All sampled rows are short; the one wide outlier sits PAST the cap and must be ignored.
		const items = Array.from({ length: cap + 100 }, (_, i) => ({ v: i === cap + 50 ? "this-is-very-long-and-should-be-ignored" : "x" }));
		const dataView = { getLength: () => items.length, getItem: (i) => items[i] };
		const w = autoFitColumnWidth(grid, dataView, { name: "v", field: "v" }, 0);
		// All sampled values are "x" (1*7 + 8 = 15 px cells). Header canvas-fallback "v" = 7 + 42 = 49 px.
		// max(49, 15) = 49; result = 49 + AUTOFIT_PADDING_PX (2) = 51 px.
		expect(w).toBe(1 * CHAR_PX + AUTOFIT_HEADER_CHROME_PX + AUTOFIT_PADDING_PX);
	});

	// Regression for "columns shrunk on initial scroll-mode load AND after fit→scroll toggle":
	// `measureFormatter` and `groupByFormatter` in `adhoc-query-grid-helper.js` return SlickGrid
	// `FormatterResultObject`s of shape `{text, toolTip}` / `{html, toolTip}`. Before the fix,
	// `measureCellsFromCanvas` did `String(rendered)` on those objects → "[object Object]" (15
	// chars), so every cell measured ~105 px regardless of content and every column shrunk to a
	// uniform narrow width. Now the formatter result is unpacked via `formatterResultToText`.
	test("formatter returning {text, toolTip} is measured by its text (NOT as '[object Object]')", () => {
		const grid = {};
		const items = [{ v: 42658 }];
		const dataView = { getLength: () => 1, getItem: (i) => items[i] };
		// Production-style measureFormatter returns an object — mimics what the heatmap formatter
		// in `adhoc-query-grid-helper.js#measureFormatter` does for numeric cells.
		const formatter = () => ({ text: "42,658.00", toolTip: 42658 });
		const w = autoFitColumnWidth(grid, dataView, { name: "v", field: "v", formatter }, 0);
		// "42,658.00" = 9 chars × 7 + 8 cell padding + 2 autofit padding = 73 px.
		// Pre-fix, this would have been "[object Object]" = 15 chars × 7 + 8 + 2 = 115 px.
		expect(w).toBe(9 * CHAR_PX + 8 + AUTOFIT_PADDING_PX);
	});

	test("formatter returning {html} is measured by the html's stripped text", () => {
		// Heatmap-style formatter that returns html only (for richer rendering).
		const grid = {};
		const items = [{ v: 100 }];
		const dataView = { getLength: () => 1, getItem: (i) => items[i] };
		const formatter = () => ({ html: '<div class="heatmap-cell"><span>100.00</span></div>', toolTip: 100 });
		const w = autoFitColumnWidth(grid, dataView, { name: "v", field: "v", formatter }, 0);
		// stripHtml('<div class="heatmap-cell"><span>100.00</span></div>') = "100.00" = 6 chars × 7 + 8 + 2 = 52 px.
		// max(header_canvas "v"=7+42=49, cells=6*7+8=50) = 50; result = 50 + 2 = 52.
		expect(w).toBe(6 * CHAR_PX + 8 + AUTOFIT_PADDING_PX);
	});

	test("formatter returning {html} prefers html over text when both are present", () => {
		// If a formatter has both `html` (richer) and `text` (short), the user sees the html — so
		// the autofit measures html (with tags stripped) to match what's visually rendered.
		const grid = {};
		const items = [{ v: 0.5 }];
		const dataView = { getLength: () => 1, getItem: (i) => items[i] };
		const formatter = () => ({
			html: '<div style="background:linear-gradient(...)"><span>50.00%</span></div>',
			text: "short",
		});
		const w = autoFitColumnWidth(grid, dataView, { name: "v", field: "v", formatter }, 0);
		// stripped html = "50.00%" = 6 chars; "short" = 5 chars. html wins → 6 × 7 + 8 + 2 = 52.
		expect(w).toBe(6 * CHAR_PX + 8 + AUTOFIT_PADDING_PX);
	});

	test("formatter that throws is gracefully handled — falls back to the raw value", () => {
		const grid = {};
		const items = [{ v: "raw-value-here" }];
		const dataView = { getLength: () => 1, getItem: (i) => items[i] };
		const formatter = () => {
			throw new Error("boom");
		};
		// Falls back to item[field] = "raw-value-here" = 14 chars. + 8 px cell padding + AUTOFIT_PADDING_PX (2).
		const w = autoFitColumnWidth(grid, dataView, { name: "v", field: "v", formatter }, 0);
		expect(w).toBe(14 * CHAR_PX + 8 + AUTOFIT_PADDING_PX);
	});

	test("null cells are skipped, not measured as 'null'", () => {
		const grid = {};
		const items = [{ v: null }, { v: undefined }, { v: "abc" }];
		const dataView = { getLength: () => items.length, getItem: (i) => items[i] };
		const w = autoFitColumnWidth(grid, dataView, { name: "v", field: "v" }, 0);
		// Only "abc" gets measured (3 chars × 7 + 8 = 29 cells); header canvas-fallback "v" = 49.
		// max(49, 29) = 49; result = 49 + AUTOFIT_PADDING_PX (2) = 51.
		expect(w).toBe(1 * CHAR_PX + AUTOFIT_HEADER_CHROME_PX + AUTOFIT_PADDING_PX);
	});

	// Regression for the "scroll-mode columns much narrower than expected" bug. With
	// `frozenColumn: 1` SlickGrid renders TWO `.slick-header-columns` containers (left = frozen,
	// right = the rest). The previous `:nth-child(N)` selector walked the whole subtree and picked
	// the wrong header (e.g. column 1's autofit measured column 2's header text), so any column
	// whose successor had a SHORTER header got under-measured. The fix routes the lookup via the
	// column id, which is stamped on the header cell as `id="<gridUid>_<colId>"`.
	// Regression for "columns shrunken on initial scroll-mode load vs after fit→scroll toggle".
	// Inside the requestAnimationFrame fired by the initial scroll-mode resync, `headerName.scrollWidth`
	// can report 0 while the cells' canvas-measured widths return positive — pre-fix the function
	// returned `cellsPx + padding` (missing the 42 px header chrome budget), producing columns
	// ~42 px narrower than the toggle path. Now the helper canvas-measures the header NAME as a
	// floor whenever DOM measurement returns 0, so the chrome budget is always included.
	test("header chrome budget is included even when DOM header reports 0 (initial-load rAF case)", () => {
		const grid = {};
		// 4-row dataView with short cells. Without the canvas-header floor, the result would be
		// `4*7 + 8 + 2 = 38 → floored to 40`. With the floor: header "loooooong-name" (14 chars × 7
		// = 98) + 42 chrome = 140; cells = 4*7 + 8 = 36; max(140, 36) = 140; result = 142.
		const items = [{ v: "vvvv" }, { v: "wwww" }, { v: "xxxx" }, { v: "yyyy" }];
		const dataView = { getLength: () => items.length, getItem: (i) => items[i] };
		const w = autoFitColumnWidth(grid, dataView, { name: "loooooong-name", field: "v" }, 0);
		expect(w).toBe(14 * CHAR_PX + AUTOFIT_HEADER_CHROME_PX + AUTOFIT_PADDING_PX);
	});

	test("frozen-pane DOM: header is matched by column id, not nth-child position", () => {
		// Two non-frozen columns sit in the right pane. The first one ("Position") has a long
		// header label; the second ("Shirt") is short. The buggy implementation would read the
		// SECOND column's header when asked to measure the FIRST one (off-by-one across panes).
		const headerByColId = {
			id_col: 60,
			Position: 220,
			Shirt: 70,
		};
		const grid = {
			getContainerNode: () => ({
				querySelector: (sel) => {
					// We get queries like `.slick-header-column[id$="_Position"] .slick-column-name`.
					// Pull the colId out of the suffix and return a stub with the expected scrollWidth.
					const m = sel.match(/_([A-Za-z0-9_]+)"\]/);
					if (!m) return null;
					const colId = m[1];
					if (headerByColId[colId] == null) return null;
					return { scrollWidth: headerByColId[colId] };
				},
			}),
		};
		// No rows — cells contribute nothing; the result is driven entirely by the header measurement
		// + AUTOFIT_HEADER_CHROME_PX (42) + AUTOFIT_PADDING_PX (2).
		const dataView = { getLength: () => 0, getItem: () => null };

		const wPosition = autoFitColumnWidth(grid, dataView, { id: "Position", name: "Position", field: "Position" }, 1);
		expect(wPosition).toBe(220 + 42 + AUTOFIT_PADDING_PX);

		const wShirt = autoFitColumnWidth(grid, dataView, { id: "Shirt", name: "Shirt", field: "Shirt" }, 2);
		expect(wShirt).toBe(70 + 42 + AUTOFIT_PADDING_PX);
	});

	test("frozen-pane DOM: column-id suffix with regex-special characters resolves via CSS.escape", () => {
		// Column ids in this codebase are usually alphanumeric, but a groupBy expression like
		// `value[USD]` could land in the id. `CSS.escape` is the safe way to embed it in an
		// attribute selector. Verify the path runs without throwing and still resolves.
		const headerByColId = {
			"value[USD]": 130,
		};
		// Stub `CSS.escape` so the stubbed `globalThis.window` is honoured (the polyfill check
		// `typeof CSS !== "undefined" && typeof CSS.escape === "function"` walks the global).
		vi.stubGlobal("CSS", {
			escape: (s) => s.replace(/\\/g, "\\\\").replace(/([\[\]])/g, "\\$1"),
		});
		const grid = {
			getContainerNode: () => ({
				querySelector: (sel) => {
					// Selector should now embed the escaped id, e.g. `_value\[USD\]"]`.
					const m = sel.match(/_(.+)"\]/);
					if (!m) return null;
					const escaped = m[1];
					const raw = escaped.replace(/\\([\[\]])/g, "$1");
					if (headerByColId[raw] == null) return null;
					return { scrollWidth: headerByColId[raw] };
				},
			}),
		};
		const dataView = { getLength: () => 0, getItem: () => null };
		const w = autoFitColumnWidth(grid, dataView, { id: "value[USD]", name: "value[USD]", field: "v" }, 1);
		expect(w).toBe(130 + 42 + AUTOFIT_PADDING_PX);
	});
});

// ---------------------------------------------------------------------------------------------
// applyAutoFitWidth — the mutation half of the auto-fit pipeline. Tests cover idempotency,
// out-of-range column-index guards, the in-place mutation contract, and the setColumns dispatch.
// ---------------------------------------------------------------------------------------------
describe("applyAutoFitWidth", () => {
	// Build a grid mock with the methods applyAutoFitWidth touches. By default the fast path
	// (`updateColumnsInternal`) is available; individual tests override it to undefined to
	// exercise the fallback (`setColumns`).
	const makeGrid = (cols, overrides = {}) => {
		const updateCalls = [];
		const setColumnsCalls = [];
		const notifyCalls = [];
		return {
			getColumns: () => cols,
			updateColumnsInternal: () => updateCalls.push(true),
			setColumns: (newCols) => setColumnsCalls.push(newCols),
			getContainerNode: () => null,
			onColumnsResized: { notify: () => notifyCalls.push(true) },
			_updateCalls: updateCalls,
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
		expect(grid._updateCalls).toHaveLength(0);
		expect(grid._setColumnsCalls).toHaveLength(0);
		expect(grid._notifyCalls).toHaveLength(0);
		expect(cols[0].width).toBe(100);
	});

	test("fast path: mutates in place + calls updateColumnsInternal (NOT setColumns)", () => {
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
		// Fast path: updateColumnsInternal is called, setColumns is NOT. setColumns adds the
		// event chain (onBeforeSetColumns / onAfterSetColumns) that fires column-picker plugins
		// and similar consumers; we don't need that on a pure width change.
		expect(grid._updateCalls).toHaveLength(1);
		expect(grid._setColumnsCalls).toHaveLength(0);
		// Notify must fire so the per-grid columnWidthMemo watcher in `adhoc-query-grid.js` picks
		// up the manual auto-fit and persists it across resyncs.
		expect(grid._notifyCalls).toHaveLength(1);
	});

	test("fallback path: setColumns when updateColumnsInternal is unavailable", () => {
		const cols = [{ id: "a", width: 100 }];
		const grid = makeGrid(cols, { updateColumnsInternal: undefined });
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
		expect(grid._updateCalls).toHaveLength(0);
		expect(grid._setColumnsCalls).toHaveLength(0);
	});
});

// ---------------------------------------------------------------------------------------------
// applyInitialScrollAutofit — the autofit orchestration for "initial scroll-mode load". Pins the
// regression for "columns shrunk on initial scroll load vs after fit→scroll toggle": the autofit
// runs against a populated dataView, every data column is sized to its cell-content width capped
// at SCROLL_MODE_AUTOFIT_CAP_PX, and the phantom trailing column is skipped.
//
// Reuses the canvas / document stubs from the autoFitColumnWidth suite at the top of this file
// (vi.stubGlobal in beforeEach). Tests run with the same CHAR_PX = 7 from that suite.
// ---------------------------------------------------------------------------------------------
describe("applyInitialScrollAutofit", () => {
	const CHAR_PX = 7;
	let savedDocument;
	let savedWindow;
	beforeEach(() => {
		savedDocument = globalThis.document;
		savedWindow = globalThis.window;
		vi.stubGlobal("document", {
			createElement: (tag) => {
				if (tag === "canvas") {
					return {
						getContext: () => ({
							font: "",
							measureText: (text) => ({ width: (text || "").length * CHAR_PX }),
						}),
					};
				}
				return {};
			},
			querySelector: () => null,
		});
		vi.stubGlobal("window", {
			getComputedStyle: () => ({ fontWeight: "400", fontSize: "13px", fontFamily: "sans-serif" }),
		});
	});
	afterEach(() => {
		vi.unstubAllGlobals();
		globalThis.document = savedDocument;
		globalThis.window = savedWindow;
	});

	test("regression: a populated dataView produces cell-driven widths (not header-only)", () => {
		// This is the test that would have caught the "still small columns" bug. Before the fix,
		// `resyncData` ran the autofit BEFORE `dataView.setItems(...)`, so `dataView.getLength()`
		// returned 0 and every cell measurement was 0 — columns ended up at ~50 px (header text +
		// chrome only). With the fix, the autofit runs AFTER setItems so cells contribute and the
		// columns reach the cell-content width.
		const items = [
			// 16 chars × 7 = 112 px cell width (+8 cell padding = 120). Header "v" is 1 char →
			// canvas-floor = 7 + 42 = 49. max(120, 49) = 120; result = 120 + 2 = 122 px.
			{ v: "some-long-value-" },
		];
		const dataView = { getLength: () => items.length, getItem: (i) => items[i] };
		const gridColumns = [
			{ id: "id", name: "#", field: "id", width: 5 },
			{ id: "v", name: "v", field: "v", width: 80 },
			{ id: "__phantom_trailing", name: "", field: "__phantom_trailing", width: 200 },
		];

		applyInitialScrollAutofit({}, dataView, gridColumns, SCROLL_MODE_AUTOFIT_CAP_PX);

		// The phantom is left alone — its width is the viewport-headroom value the resync code set.
		expect(gridColumns[2].width).toBe(200);
		// The "v" column is sized to the cell content, NOT to "header + chrome only" (~50 px).
		expect(gridColumns[1].width).toBe(16 * CHAR_PX + 8 + AUTOFIT_PADDING_PX);
		expect(gridColumns[1].width).toBeGreaterThan(60); // sanity: definitely not the header-only fallback
	});

	test("BUG REPRO: an empty dataView yields the bug's narrow widths — exercise the failure mode", () => {
		// Captures the bad path so future regressions of resyncData's sequencing are caught: an
		// EMPTY dataView at autofit time produces "header + chrome" widths because no cells exist
		// to widen the column. The production fix is to populate the dataView FIRST in
		// `resyncData` so this branch never runs against an empty view; this test pins the failure
		// mode so the next time someone re-orders resyncData, they see why it matters.
		const dataView = { getLength: () => 0, getItem: () => null };
		const gridColumns = [
			{ id: "v", name: "v", field: "v", width: 80 },
			{ id: "__phantom_trailing", name: "", field: "__phantom_trailing", width: 200 },
		];

		applyInitialScrollAutofit({}, dataView, gridColumns, SCROLL_MODE_AUTOFIT_CAP_PX);

		// 1 char × 7 + 42 chrome + 2 padding = 51 px. THIS is the "shrunk columns" the user saw.
		expect(gridColumns[0].width).toBe(1 * CHAR_PX + AUTOFIT_HEADER_CHROME_PX + AUTOFIT_PADDING_PX);
	});

	test("respects the cap: a column with very long cells is clamped to SCROLL_MODE_AUTOFIT_CAP_PX", () => {
		const longString = "x".repeat(200); // 200 chars × 7 = 1400 px, way over cap
		const items = [{ v: longString }];
		const dataView = { getLength: () => items.length, getItem: (i) => items[i] };
		const gridColumns = [{ id: "v", name: "v", field: "v", width: 80 }];

		applyInitialScrollAutofit({}, dataView, gridColumns, SCROLL_MODE_AUTOFIT_CAP_PX);

		expect(gridColumns[0].width).toBe(SCROLL_MODE_AUTOFIT_CAP_PX);
	});

	test("phantom trailing column is left at its incoming width (never measured)", () => {
		const items = [{ v: "abc" }];
		const dataView = { getLength: () => items.length, getItem: (i) => items[i] };
		const gridColumns = [
			{ id: "v", name: "v", field: "v", width: 80 },
			{ id: "__phantom_trailing", name: "", field: "__phantom_trailing", width: 333 },
		];

		applyInitialScrollAutofit({}, dataView, gridColumns, SCROLL_MODE_AUTOFIT_CAP_PX);

		expect(gridColumns[1].width).toBe(333);
	});

	test("a column with autoFitColumnWidth returning 0 keeps its incoming width (skip-on-failure)", () => {
		// `autoFitColumnWidth` returns 0 only when canvas context is unavailable AND the dataView
		// path yields nothing. We simulate that by stubbing canvas createElement to fail.
		vi.unstubAllGlobals();
		vi.stubGlobal("document", {
			createElement: () => ({ getContext: () => null }),
			querySelector: () => null,
		});
		vi.stubGlobal("window", { getComputedStyle: () => ({ fontWeight: "400", fontSize: "13px", fontFamily: "sans-serif" }) });

		const dataView = { getLength: () => 1, getItem: () => ({ v: "abc" }) };
		const gridColumns = [{ id: "v", name: "v", field: "v", width: 80 }];
		applyInitialScrollAutofit({}, dataView, gridColumns, SCROLL_MODE_AUTOFIT_CAP_PX);
		// Width left unchanged when autofit returns 0.
		expect(gridColumns[0].width).toBe(80);
	});
});
