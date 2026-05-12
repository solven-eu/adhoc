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

/** Padding added to the measured pixel width to leave breathing room — accounts for SlickGrid's cell
 *  padding (~6px left + 2px right) plus a couple of pixels so the text doesn't render right against the
 *  resize handle. */
export const AUTOFIT_PADDING_PX = 16;

/** Floor for the computed width — narrower than this and the resize handle becomes hard to grab. */
export const AUTOFIT_MIN_WIDTH_PX = 40;

/** Strip HTML tags from a rendered cell value so the canvas measure reflects the visible text only.
 *  Cell formatters in this codebase often emit `<span>` / `<i>` wrappers; without this strip, the angle
 *  brackets count towards the measured width and produce ridiculous results. */
export function stripHtml(value) {
	if (value == null) {
		return "";
	}
	return String(value).replace(/<[^>]+>/g, "");
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
	// Preferred path: probe the live DOM. Each rendered cell has overflow:hidden in SlickGrid's CSS,
	// so `scrollWidth` reports the cell's NATURAL width — exactly what we need to "unsqueeze" it.
	const fromDom = measureFromDom(grid, colIdx);
	if (fromDom > 0) {
		return Math.max(AUTOFIT_MIN_WIDTH_PX, Math.ceil(fromDom) + AUTOFIT_PADDING_PX);
	}
	// Fallback: canvas-text-measure path. Used when no row is rendered yet (e.g. the grid was just
	// created with zero rows) and during unit tests where we stub out the document API.
	return measureFromCanvas(dataView, column, colIdx, grid);
}

/**
 * Walk the DOM under the grid root, find the rendered header AND every visible cell for the given
 * column index, and return the maximum `scrollWidth` — i.e. the width each element would need to
 * stop truncating. Returns 0 if the grid root, header, or cells aren't in the document yet.
 */
function measureFromDom(grid, colIdx) {
	const root = typeof grid.getContainerNode === "function" ? grid.getContainerNode() : null;
	if (!root || typeof root.querySelectorAll !== "function") {
		return 0;
	}
	let max = 0;

	// Header — we measure the OUTER `.slick-header-column` (not just the inner `.slick-column-name`
	// span) because the cell includes left/right padding and a sort-indicator slot whose width was
	// being missed by the inner-span probe, leaving the resulting column too narrow to show the
	// header text fully.
	const header = root.querySelector(`.slick-header-column:nth-child(${colIdx + 1})`);
	if (header && header.scrollWidth) {
		max = header.scrollWidth;
	}

	// Cells. SlickGrid lazy-renders rows: only visible ones are in the DOM. That gives us a natural
	// performance cap that mirrors AUTOFIT_MAX_ROWS_PROBED — we don't probe rows that aren't visible
	// anyway. The cell selectors stamp `.l{colIdx}` and `.r{colIdx}` classes (left/right cell index).
	const cells = root.querySelectorAll(`.slick-cell.l${colIdx}`);
	for (const cell of cells) {
		if (cell.scrollWidth > max) {
			max = cell.scrollWidth;
		}
	}
	return max;
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

	// Cell-side update via the light path. applyColumnWidths is SlickGrid's "I changed widths,
	// repaint cells" entry point — much cheaper than setColumns (which re-renders every row).
	if (typeof grid.applyColumnWidths === "function") {
		grid.applyColumnWidths();
	} else {
		grid.setColumns(cols);
	}

	// Header-side update: applyColumnWidths only repaints cells on the SlickGrid build we ship.
	// Manually patch the matching `.slick-header-column` width attribute and inline style so the
	// header aligns with the cells. `updateColumnHeader` would also work but isn't available on
	// every build.
	if (typeof grid.getContainerNode === "function") {
		const root = grid.getContainerNode();
		if (root && typeof root.querySelector === "function") {
			const headerCell = root.querySelector(`.slick-header-column:nth-child(${colIdx + 1})`);
			if (headerCell) {
				headerCell.style.width = newWidth + "px";
			}
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
