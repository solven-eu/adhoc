// @ts-check
import { expect, test, vi } from "vitest";

// `adhoc-query-grid-helper.js` is browser code: it imports a few bare module specifiers (slickgrid,
// sortablejs, bootstrap) that the SPA resolves via the runtime importmap but vitest cannot find
// under `node_modules`, AND it touches `window` at module load (`window.Sortable = Sortable`).
// None of those are actually exercised by the formatter functions we want to test — they just have
// to be loadable. We mock the bare imports with minimal shims and stub `window` via vi.hoisted so
// the assignment at line ~22 succeeds without a DOM environment.
vi.hoisted(() => {
	globalThis.window = globalThis.window || /** @type {any} */ ({});
});
vi.mock("sortablejs", () => ({ default: {} }));
vi.mock("slickgrid", () => ({ SlickHeaderButtons: function () {} }));
vi.mock("bootstrap", () => ({ Modal: function () {} }));

import gridHelper from "@/js/adhoc-query-grid-helper.js";

// These tests exercise the three flavours of cell rendering driven by the grid helper:
//   - `groupByFormatter`  — built per query via gridHelper.groupByToGridColumns(...).formatter
//   - `measureFormatter`  — built via gridHelper.formatters(...).measureFormatter
//   - `percentFormatter`  — built via gridHelper.formatters(...).percentFormatter
//
// We assert the user-visible contract: missing → `NULL`, empty string → `empty`, the engine-side
// grand-total marker `*` → `Total`, and the DT-only "Out of DT" placeholder for columns the user
// did not request. The tests deliberately exercise non-DT views too (since `NULL` / `empty` /
// `Total` are not DT-specific) and pin the documented collision between a real `*` coordinate and
// the synthetic grand-total marker.

const groupByFormatterFor = function (isDrillthrough, requestedColumns) {
	const columns = gridHelper.groupByToGridColumns(["country"], null, null, isDrillthrough, requestedColumns);
	return columns[0].formatter;
};

/**
 * @param {{ isDrillthrough: boolean, requestedColumns?: any }} opts
 */
const measureFormatters = function ({ isDrillthrough, requestedColumns }) {
	return gridHelper.formatters({}, null, null, null, isDrillthrough || false, requestedColumns);
};

test("groupBy formatter: non-DT view renders null as the greyed `NULL` placeholder", () => {
	const fmt = groupByFormatterFor(false, undefined);
	const cell = fmt(0, 0, null, { id: "country" });
	expect(cell.html).toContain("NULL");
	expect(cell.html).toContain("font-style:italic");
	expect(cell.toolTip).toMatch(/null/i);
});

test("groupBy formatter: non-DT view renders empty string as the greyed `empty` placeholder", () => {
	const fmt = groupByFormatterFor(false, undefined);
	const cell = fmt(0, 0, "", { id: "country" });
	expect(cell.html).toContain("empty");
	expect(cell.html).toContain("font-style:italic");
	expect(cell.toolTip).toMatch(/empty/i);
});

test("groupBy formatter: real coordinate is rendered as plain text", () => {
	const fmt = groupByFormatterFor(false, undefined);
	const cell = fmt(0, 0, "France", { id: "country" });
	expect(cell.text).toBe("France");
	expect(cell.toolTip).toBe("France");
});

test("groupBy formatter: grand-total marker `*` is rendered as `Total`", () => {
	const fmt = groupByFormatterFor(false, undefined);
	const cell = fmt(0, 0, "*", { id: "country" });
	expect(cell.text).toBe("Total");
	expect(cell.toolTip).toMatch(/grand-total/i);
});

test("groupBy formatter: DT view distinguishes `Out of DT` for columns not in requestedColumns", () => {
	const requested = new Set(["country"]);
	const fmt = groupByFormatterFor(true, requested);

	// `country` IS requested → null renders as `NULL`.
	const requestedCell = fmt(0, 0, null, { id: "country" });
	expect(requestedCell.html).toContain("NULL");

	// `inflation` is NOT requested → null/empty renders as `Out of DT` regardless of value shape.
	const extraColumnFmt = groupByFormatterFor(true, requested);
	const outOfDtNull = extraColumnFmt(0, 0, null, { id: "inflation" });
	expect(outOfDtNull.html).toContain("Out of DT");
	const outOfDtEmpty = extraColumnFmt(0, 0, "", { id: "inflation" });
	expect(outOfDtEmpty.html).toContain("Out of DT");
});

test("groupBy formatter: a real coordinate equal to `*` collides with the synthetic grand-total — both render as `Total`", () => {
	// Documented limitation: the engine uses the literal string `*` as the CalculatedCoordinate
	// marker for the grand-total rollup. If a data row happens to carry the literal string `*`
	// for a column (e.g. a country named "*"), the renderer CANNOT distinguish it from the
	// synthetic rollup row — both surface as the same `Total` cell. This test pins the current
	// behaviour so a future change that decides to address the collision (separate marker, distinct
	// styling, or a warning) has to consciously update this expectation.
	const fmt = groupByFormatterFor(false, undefined);
	const syntheticGrandTotal = fmt(0, 0, "*", { id: "country" });
	const realStarCoordinate = fmt(0, 0, "*", { id: "country" });

	expect(syntheticGrandTotal.text).toBe("Total");
	expect(realStarCoordinate.text).toBe("Total");
	// Both share the same tooltip — no field on the cell tells them apart.
	expect(syntheticGrandTotal).toEqual(realStarCoordinate);
});

test("measure formatter: null value renders as `NULL` placeholder, in non-DT views too", () => {
	const { measureFormatter } = measureFormatters({ isDrillthrough: false });
	const cell = measureFormatter(0, 0, null, { id: "delta" }, {});
	expect(cell.html).toContain("NULL");
});

test("measure formatter: empty string renders as `empty` placeholder, in non-DT views too", () => {
	const { measureFormatter } = measureFormatters({ isDrillthrough: false });
	const cell = measureFormatter(0, 0, "", { id: "delta" }, {});
	expect(cell.html).toContain("empty");
});

test("measure formatter: numeric value passes through the regular number format", () => {
	const { measureFormatter } = measureFormatters({ isDrillthrough: false });
	const cell = measureFormatter(0, 0, 1234.5, { id: "delta" }, {});
	// Locale-dependent: we assert that the digits made it through rather than the exact separator.
	expect(String(cell.text)).toMatch(/1.?234/);
});

test("measure formatter: DT view yields `Out of DT` for null on a column outside the user's query", () => {
	const requested = new Set(["delta"]);
	const { measureFormatter } = measureFormatters({ isDrillthrough: true, requestedColumns: requested });
	const cell = measureFormatter(0, 0, null, { id: "theta" }, {});
	expect(cell.html).toContain("Out of DT");
});

test("percent formatter: null and empty are handled identically to measure formatter", () => {
	const { percentFormatter } = measureFormatters({ isDrillthrough: false });
	expect(percentFormatter(0, 0, null, { id: "m%" }, {}).html).toContain("NULL");
	expect(percentFormatter(0, 0, "", { id: "m%" }, {}).html).toContain("empty");
});
