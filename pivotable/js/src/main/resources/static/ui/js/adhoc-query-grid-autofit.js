// @ts-check
// Excel-like column auto-fit for the SlickGrid-based pivot grid. SlickGrid does not provide this
// out of the box — we delegate a `dblclick` listener on the grid container, target
// `.slick-resizable-handle` (the 4-px-wide strip on the right edge of each header cell), measure
// the widest rendered string across the column's header + every sampled row, then apply the new
// width via `grid.setColumns(...)`.
//
// Extracted from `adhoc-query-grid-helper.js` so the pure measurement logic can be unit-tested
// without dragging in the grid-wide module's `sortablejs` / SlickGrid imports.

/** Cap on how many rows we sample when computing an auto-fit width — keeps the dblclick instantaneous on
 *  large views. 99% of the time the widest cell is in the first screenful anyway; outliers further down
 *  can still be revealed by a manual drag. */
export const AUTOFIT_MAX_ROWS_PROBED = 200;

/** Padding added on TOP of the measured `scrollWidth` to leave a sliver of breathing room.
 *  Deliberately tiny: `scrollWidth` already includes SlickGrid's cell padding (~6px left + 2px
 *  right), so adding any more than 2–3 px here makes the column visibly looser than the cells
 *  actually need. The previous value (16 px) caused dblclick to INCREASE the column even when
 *  the content was already comfortable. */
export const AUTOFIT_PADDING_PX = 2;

/** Pixel budget reserved for the header's static chrome: sort-indicator slot + 3-dot action menu icon
 *  + header padding. Counted IN ADDITION to the measured `.slick-column-name` scrollWidth (which
 *  itself already includes the inline clipboard copy-name icon).
 *
 *  The 3-dot menu is only visible on hover, but we still reserve space for it here so the auto-fit
 *  width never under-shoots the rendered header: a column auto-fitted to "just the name + clipboard
 *  icon" would visually overflow the moment the user hovered the header and the 3-dot icon faded
 *  in, popping the name text into an ellipsis. Reserving the slot keeps the header stable in both
 *  states. ~12 px for the sort indicator slot, ~22 px for the 3-dot icon, ~8 px for padding. */
export const AUTOFIT_HEADER_CHROME_PX = 42;

/** Floor for the computed width — narrower than this and the resize handle becomes hard to grab. */
export const AUTOFIT_MIN_WIDTH_PX = 40;

/**
 * Cap applied when auto-sizing columns during the fit→scroll transition. SlickGrid's scroll mode lets
 * any column take as much horizontal real-estate as the content needs; for wide cells (long text,
 * 20-character measure names) the natural content width can easily exceed the viewport, making the
 * scroll-mode grid effectively single-column. 300 px is wide enough for typical labels (~40 chars in
 * a 14 px font) yet keeps a multi-column horizontal layout in view.
 */
export const SCROLL_MODE_AUTOFIT_CAP_PX = 300;

/** Strip HTML tags from a rendered cell value so the canvas measure reflects the visible text only.
 *  Cell formatters in this codebase often emit `<span>` / `<i>` wrappers; without this strip, the angle
 *  brackets count towards the measured width and produce ridiculous results.
 *
 *  <p>The replace loop is INTENTIONALLY iterative rather than a single `replace` call: an attacker-
 *  crafted (or accidentally pathological) payload like `<scr<script>ipt>alert(1)</scr</script>ipt>`
 *  collapses to `<script>alert(1)</script>` after one pass of the regex — a still-functional script
 *  tag that a downstream text-based check might rule "safe". Keep replacing until a pass produces
 *  no change, so the result is genuinely tag-free regardless of nesting. Flagged by CodeQL's
 *  `js/incomplete-multi-character-sanitization` rule. */
export function stripHtml(value) {
	if (value == null) {
		return "";
	}
	let text = String(value);
	let previous;
	do {
		previous = text;
		text = text.replace(/<[^>]+>/g, "");
	} while (text !== previous);
	return text;
}

/**
 * Compute the minimum column width (in pixels) needed to render the column's header AND every visible
 * cell's value without truncation. Reads `scrollWidth` directly from the rendered DOM elements so the
 * result matches the actual painted layout — including the header chrome (sort indicator, copy icon),
 * cell padding, and any formatter markup the canvas-based path used to under-count.
 *
 * Falls back to the canvas-text-measure path when no rendered header / cells are in the DOM yet,
 * which keeps unit tests working without a real grid.
 *
 * @returns the optimal width in CSS pixels, or 0 when the column is missing
 */
export function autoFitColumnWidth(grid, dataView, column, colIdx) {
	if (!column) {
		return 0;
	}
	// Header → measure the DOM (the inner `.slick-column-name` scrollWidth is reliable, header text
	// is short enough that `overflow: hidden` rarely lies). Cells → measure the FORMATTER OUTPUT via
	// canvas: `cell.scrollWidth` returns the cell's CURRENT width when content fits comfortably
	// (not the natural content width), so a DOM-based cell measurement perpetuates whatever
	// over-wide width the cell currently has. Canvas text-measure is independent of the rendered
	// cell width and gives the true content width.
	const headerPx = measureHeaderFromDom(grid, colIdx);
	const cellsPx = measureCellsFromCanvas(dataView, column, colIdx, grid);
	const measuredMax = Math.max(headerPx, cellsPx);
	if (measuredMax <= 0) {
		// Initial-paint case (no DOM, no rows). Fall back to the all-canvas path which measures
		// the header text via canvas too — same as the previous behaviour.
		return measureFromCanvas(dataView, column, colIdx, grid);
	}
	return Math.max(AUTOFIT_MIN_WIDTH_PX, Math.ceil(measuredMax) + AUTOFIT_PADDING_PX);
}

/**
 * Walk the DOM under the grid root, find the rendered header's inner `.slick-column-name` for the
 * given column index, and return its `scrollWidth` plus a reserved budget for the surrounding chrome
 * (sort indicator + 3-dot action menu icon + header padding). Returns 0 if the grid root or header
 * is not in the document yet — the caller falls back to canvas-text measurement.
 */
function measureHeaderFromDom(grid, colIdx) {
	const root = typeof grid.getContainerNode === "function" ? grid.getContainerNode() : null;
	if (!root || typeof root.querySelector !== "function") {
		return 0;
	}
	// Measure the INNER `.slick-column-name` (just the name + the sort-indicator slot) and add
	// `AUTOFIT_HEADER_CHROME_PX` for the padding. The outer `.slick-header-column` would include
	// the 3-dot action menu icon, which is `display: none` off-hover but becomes visible (and
	// therefore part of scrollWidth) during the dblclick interaction itself — using that outer
	// measurement makes the column inflate by ~24 px every dblclick even when the content is
	// comfortable in the current width.
	const headerName = root.querySelector(`.slick-header-column:nth-child(${colIdx + 1}) .slick-column-name`);
	if (!headerName || !headerName.scrollWidth) {
		return 0;
	}
	return headerName.scrollWidth + AUTOFIT_HEADER_CHROME_PX;
}

/**
 * Measure the widest formatted cell value in the column via Canvas2D text measurement. Returns the
 * pixel width (including SlickGrid's per-cell horizontal padding), or 0 when the dataView is empty
 * or no canvas context can be obtained.
 *
 * <p>We deliberately do NOT read `cell.scrollWidth` from the DOM here: when the cell content fits
 * comfortably, `scrollWidth` returns the cell's CURRENT rendered width — so an already-too-wide
 * column would auto-fit to its own current width and never shrink. Canvas `measureText` reports
 * the natural content width, independent of how SlickGrid is currently rendering the cell.
 */
function measureCellsFromCanvas(dataView, column, colIdx, grid) {
	if (!dataView || typeof dataView.getLength !== "function") {
		return 0;
	}
	const canvas = document.createElement("canvas");
	const ctx = canvas.getContext("2d");
	if (!ctx) {
		return 0;
	}
	// Read the grid's font from a real cell so the canvas measurement matches the painted text
	// width to the pixel. Fall back to the header's font, then to a sensible default.
	const sampleEl = document.querySelector(".slick-cell") || document.querySelector(".slick-header-column .slick-column-name");
	if (sampleEl) {
		const cs = window.getComputedStyle(sampleEl);
		ctx.font = `${cs.fontWeight} ${cs.fontSize} ${cs.fontFamily}`;
	} else {
		ctx.font = "13px sans-serif";
	}

	let maxPx = 0;
	const rowCount = Math.min(dataView.getLength(), AUTOFIT_MAX_ROWS_PROBED);
	for (let r = 0; r < rowCount; r++) {
		const item = dataView.getItem(r);
		if (!item) continue;
		let rendered;
		if (typeof column.formatter === "function") {
			try {
				rendered = column.formatter(r, colIdx, item[column.field], column, item, grid);
			} catch {
				rendered = item[column.field];
			}
		} else {
			rendered = item[column.field];
		}
		if (rendered == null) continue;
		const w = ctx.measureText(stripHtml(rendered)).width;
		if (w > maxPx) {
			maxPx = w;
		}
	}

	// SlickGrid cells have ~8 px of horizontal padding (6 left + 2 right) baked into the cell box.
	// `measureText` returns just the text width, so we need to add this to land on a column width
	// the cell can comfortably display.
	const CELL_INNER_PADDING_PX = 8;
	return maxPx > 0 ? maxPx + CELL_INNER_PADDING_PX : 0;
}

/**
 * Canvas-text-measure path. Iterates the dataView with the column's formatter to construct a string
 * for every visible row, strips HTML, then asks Canvas2D for each string's width. Capped to
 * {@link AUTOFIT_MAX_ROWS_PROBED} rows for performance.
 */
function measureFromCanvas(dataView, column, colIdx, grid) {
	const canvas = document.createElement("canvas");
	const ctx = canvas.getContext("2d");
	if (!ctx) {
		return 0;
	}
	const headerEl = document.querySelector(".slick-header-column .slick-column-name") || document.querySelector(".slick-cell");
	if (headerEl) {
		const cs = window.getComputedStyle(headerEl);
		ctx.font = `${cs.fontWeight} ${cs.fontSize} ${cs.fontFamily}`;
	} else {
		ctx.font = "13px sans-serif";
	}

	const measure = (text) => ctx.measureText(stripHtml(text)).width;

	let maxPx = measure(column.name || column.field || "");

	const rowCount = Math.min(dataView.getLength(), AUTOFIT_MAX_ROWS_PROBED);
	for (let r = 0; r < rowCount; r++) {
		const item = dataView.getItem(r);
		if (!item) {
			continue;
		}
		let rendered;
		if (typeof column.formatter === "function") {
			try {
				rendered = column.formatter(r, colIdx, item[column.field], column, item, grid);
			} catch {
				rendered = item[column.field];
			}
		} else {
			rendered = item[column.field];
		}
		if (rendered == null) {
			continue;
		}
		const w = measure(rendered);
		if (w > maxPx) {
			maxPx = w;
		}
	}
	return Math.max(AUTOFIT_MIN_WIDTH_PX, Math.ceil(maxPx) + AUTOFIT_PADDING_PX);
}

/**
 * Apply an auto-fit width to a single column. Idempotent: returns `false` (a no-op) when the
 * column already sits at the target width — useful so callers can decide whether to `preventDefault`
 * the originating event.
 *
 * <p>Performance contract: this path is on the dblclick UX, so it must be cheap even on large
 * grids. We mutate the column in place and call the LIGHT update path:
 * <ul>
 *   <li>{@code applyColumnWidths()} — rewrites the CSS rule that drives cell widths. Doesn't
 *       re-render rows.</li>
 *   <li>Plus a manual DOM patch of the matching {@code .slick-header-column} so the header
 *       width stays aligned (applyColumnWidths only repaints cells).</li>
 * </ul>
 * The previous implementation used {@code grid.setColumns(cols)} which re-renders every row and
 * was 1-3 seconds slow on a busy grid.
 *
 * @returns true if a width change was applied; false on a no-op
 */
export function applyAutoFitWidth(grid, colIdx, newWidth) {
	if (!grid || typeof grid.getColumns !== "function") {
		return false;
	}
	const cols = grid.getColumns();
	if (colIdx < 0 || colIdx >= cols.length) {
		return false;
	}
	if (cols[colIdx].width === newWidth) {
		// Idempotency: same width as currently displayed → nothing to do. Without this guard,
		// repeated dblclicks would still trigger repaint events even when the result is identical.
		return false;
	}
	cols[colIdx].width = newWidth;

	// Width-only update path: rebuild the per-column CSS rules + reapply widths to the existing
	// header / cell / footer DOM, WITHOUT recreating those rows. SlickGrid's higher-level entry
	// points (`setColumns` and `updateColumnsInternal`) both call `createColumnHeaders()` AND
	// `createColumnFooter()`, which empty + recreate the header and footer DOM — emptying any
	// content that lived in those cells (the min/max footer summary populated by
	// `gridHelper.updateFooters` in `adhoc-query-grid.js`). Since auto-fit only changes a single
	// column's width, the row DOM is already correct; rebuilding it costs ~1-3 s on large grids
	// AND drops the footer content. The sequence below mirrors what `updateColumnsInternal` does
	// for the WIDTH portion but skips the structural-rebuild calls.
	const tryWidthOnlyUpdate = () => {
		const methods = ["removeCssRules", "createCssRules", "updateCanvasWidth", "applyColumnHeaderWidths", "applyColumnWidths", "resizeCanvas"];
		for (const m of methods) {
			if (typeof grid[m] !== "function") {
				return false;
			}
		}
		grid.removeCssRules();
		grid.createCssRules();
		grid.updateCanvasWidth();
		grid.applyColumnHeaderWidths();
		grid.applyColumnWidths();
		grid.resizeCanvas();
		return true;
	};
	if (!tryWidthOnlyUpdate()) {
		// Fallback: builds that don't expose the low-level methods — accept the footer wipe to
		// keep the column resize working at all.
		if (typeof grid.updateColumnsInternal === "function") {
			grid.updateColumnsInternal();
		} else {
			grid.setColumns(cols);
		}
	}

	// Notify the same listeners a manual drag would, so the columnWidthMemo watcher in the
	// grid setup picks up the change.
	if (grid.onColumnsResized && typeof grid.onColumnsResized.notify === "function") {
		grid.onColumnsResized.notify({ grid }, null, grid);
	}

	return true;
}

/**
 * Attach the dblclick auto-fit listener to a mounted SlickGrid instance. Safe to call once per grid
 * lifetime — DOM listeners are scoped to the grid's container node and torn down when the node is
 * removed from the document.
 */
export function registerHeaderResizeAutoFit(grid, dataView) {
	// SlickGrid 4 exposes `getContainerNode()`; the resize handles are descendants of that node so
	// delegating from the container catches every column without per-column wiring. Falling back to
	// the canvas's grandparent keeps the feature alive on older builds.
	let root = typeof grid.getContainerNode === "function" ? grid.getContainerNode() : null;
	if (!root && typeof grid.getCanvasNode === "function") {
		const canvas = grid.getCanvasNode();
		root = canvas && canvas.parentNode ? canvas.parentNode.parentNode : null;
	}
	if (!root) {
		console.warn("registerHeaderResizeAutoFit: cannot locate grid container — auto-fit disabled");
		return;
	}

	root.addEventListener("dblclick", (e) => {
		const handle = e.target.closest && e.target.closest(".slick-resizable-handle");
		if (!handle) {
			return;
		}
		const headerCell = handle.closest(".slick-header-column");
		if (!headerCell) {
			return;
		}
		// SlickGrid 4 stamps the column id on the header cell via its `id` attribute (suffix after the
		// last `_`). Fall back to sibling-index when not present.
		let colIdx = -1;
		const id = headerCell.id || "";
		const cols = grid.getColumns();
		if (id) {
			const colId = id.substring(id.lastIndexOf("_") + 1);
			colIdx = cols.findIndex((c) => String(c.id) === colId);
		}
		if (colIdx < 0) {
			const siblings = Array.from(headerCell.parentNode.children).filter((c) => c.classList.contains("slick-header-column"));
			colIdx = siblings.indexOf(headerCell);
		}
		if (colIdx < 0 || colIdx >= cols.length) {
			return;
		}

		const newWidth = autoFitColumnWidth(grid, dataView, cols[colIdx], colIdx);
		if (newWidth <= 0) {
			return;
		}
		const applied = applyAutoFitWidth(grid, colIdx, newWidth);
		if (applied) {
			// Suppress default browser handling of the dblclick (e.g. selecting nearby text) and
			// stop propagation so the cell-modal opener doesn't also fire on a header-handle dblclick.
			e.preventDefault();
			e.stopPropagation();
		}
	});
}
