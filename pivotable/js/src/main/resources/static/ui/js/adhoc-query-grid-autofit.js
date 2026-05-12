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
 * cell's value without truncation. Returns 0 when the column is missing or the canvas API isn't
 * available. Bounded to {@link AUTOFIT_MAX_ROWS_PROBED} rows for performance.
 */
export function autoFitColumnWidth(grid, dataView, column, colIdx) {
	if (!column) {
		return 0;
	}
	const canvas = document.createElement("canvas");
	const ctx = canvas.getContext("2d");
	if (!ctx) {
		return 0;
	}
	// Read the grid's actual font from a header DOM node so the measurement matches what's painted.
	// Falling back to a sensible default keeps the feature usable when the DOM probe fails (e.g.
	// the grid has no rendered header yet).
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

		// Clone the columns array (SlickGrid mutates it internally) and replace the targeted column
		// with the freshly-sized one. setColumns triggers a redraw + an `onColumnsResized` event so
		// subscribers (e.g. preferences persistence) get notified the same as a manual drag.
		const newCols = cols.map((c, i) => (i === colIdx ? { ...c, width: newWidth } : c));
		grid.setColumns(newCols);

		// Suppress default browser handling of the dblclick (e.g. selecting nearby text) and stop
		// propagation so the cell-modal opener doesn't also fire on a header-handle dblclick.
		e.preventDefault();
		e.stopPropagation();
	});
}
