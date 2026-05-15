// @ts-check
import { ref, computed, watch, onMounted, reactive, provide, inject } from "vue";

import AdhocCellModal from "./adhoc-query-grid-cell-modal.js";
import AdhocGridTimingsBar from "./adhoc-query-grid-timings-bar.js";
import AdhocGridControls from "./adhoc-query-grid-controls.js";
import AdhocMeasureStatsModal from "./adhoc-query-grid-stats-modal.js";

// Formatters
import { SlickGrid, SlickDataView } from "slickgrid";

import gridHelper from "./adhoc-query-grid-helper.js";
import { autoFitColumnWidth, SCROLL_MODE_AUTOFIT_CAP_PX } from "./adhoc-query-grid-autofit.js";
import { isLoading as isLoadingHelper, loadingPercent as loadingPercentHelper, loadingMessage as loadingMessageHelper } from "./adhoc-query-grid-loading.js";

import { usePreferencesStore } from "./store-preferences.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocCellModal,
		AdhocGridTimingsBar,
		AdhocGridControls,
		AdhocMeasureStatsModal,
	},
	// https://vuejs.org/guide/components/props.html
	props: {
		domId: {
			type: String,
			required: true,
		},
		tabularView: {
			type: Object,
			required: true,
		},
		loading: {
			type: Boolean,
			default: false,
		},
		// Used to remove columns from the grid
		queryModel: {
			type: Object,
			required: false,
		},
		// Used to feed the cellModal with cube details, like measures underlyings
		cube: {
			type: Object,
			required: true,
		},
		// Identifies the cube for per-cube preferences (column widths, …). Both optional to keep
		// the grid usable without persistence wiring (e.g. in isolated previews).
		endpointId: {
			type: String,
			default: "",
		},
		cubeId: {
			type: String,
			default: "",
		},
		// Shared model owned by the parent (<AdhocQuery>): the grid pushes the scroll-mode
		// offscreen-columns count and a `scrollToRightEnd` action into it, so the parent can
		// render the "+N more" chip in the LiveView strip rather than inside the grid container
		// (where it would be clipped by the grid's own overflow). Optional to keep the grid
		// usable in isolation (e.g. tests).
		gridShared: {
			type: Object,
			default: null,
		},
	},
	setup(props) {
		// https://stackoverflow.com/questions/2402953/javascript-data-grid-for-millions-of-rows
		let dataView;

		// Shared store: read once at setup, consulted from `resyncData` (gridLayout) and from the
		// `wizardHidden` / `gridLayout` watchers in `onMounted`. Pinia memoises the store so the same
		// reactive instance is returned across calls — the dedicated `preferencesStoreForResize`
		// reference inside `onMounted` is the same object as this one.
		const preferencesStore = usePreferencesStore();

		// Singleton model for the per-measure Statistics modal — provided by `adhoc-query.js`
		// so the grid header buttons (registered in `adhoc-query-grid-helper.js`) can toggle
		// it without prop-drilling. Read here so the local AdhocMeasureStatsModal gets the
		// same reactive object the header buttons mutate.
		const measureStatsModel = inject("measureStatsModel");

		let grid;
		/** @type {any} reactive bag of grid-side metadata populated incrementally by resyncData (`nb_rows`, etc.) */
		const gridMetadata = reactive({});
		// Two distinct "empty" states with different UX:
		//
		//   - `isEmptyModel` — the queryModel itself has no selected measures and no
		//     selected columns (the user hasn't built a query yet). This is the case
		//     that warrants the wizard-pointer hint ("Use the wizard…"). Derived as a
		//     `computed` so it tracks the queryModel reactively without being mutated
		//     from resyncData.
		//
		//   - `isEmptyView` — the user HAS built a query, but the backend returned zero
		//     rows. The grid still shows its column headers; we don't want the wizard
		//     hint here (it would lie about the state) but we may want a different
		//     "no rows match" message in the future.
		const isEmptyModel = computed(() => {
			const qm = props.queryModel;
			if (!qm) return true;
			const hasSelectedMeasure = Object.values(qm.selectedMeasures || {}).some((v) => v === true);
			const hasSelectedColumn = Object.values(qm.selectedColumns || {}).some((v) => v === true);
			return !hasSelectedMeasure && !hasSelectedColumn;
		});
		const isEmptyView = computed(() => !isEmptyModel.value && !gridMetadata.nb_rows);

		const formatOptions = reactive({
			// https://stackoverflow.com/questions/673905/how-can-i-determine-a-users-locale-within-the-browser
			locale: navigator.languages && navigator.languages.length ? navigator.languages[0] : navigator.language,
			// May defaulted to `EUR`
			measureCcy: "",
			// After this number of digits, numbers are simplified with `0`s.
			// TODO This is disabled as it leads to unexpected formatting even with `roundingPriority=morePrecision`
			// e.g. `12.3` is formatted `12.3` instead of `12.30`
			// maximumSignificantDigits: 9,
			// Minumum number of decimals
			minimumFractionDigits: 2,
			// Maximum number of decimals
			maximumFractionDigits: 2,
			// Default is 'auto'
			// roundingPriority: 'morePrecision',

			// Heatmaps are independently togglable, both OFF by default — the grid reads like a plain
			// table on first render. Users opt into each from the Formatting Options modal.
			//   - primaryHeatmap   : green→red cell-background gradient calibrated against the
			//                        column's full min/max (via `heatmapColor()`).
			//   - secondaryHeatmap : horizontal in-cell bar calibrated against the parent-slice
			//                        group (via `secondaryHeatmapFill()`). Useful when a query has
			//                        a hierarchical groupBy and you want to see each row's weight
			//                        within its parent rather than against the whole column.
			// When both are false, the measure/percent formatters skip all heatmap helpers entirely
			// and return plain-text cells.
			primaryHeatmap: false,
			secondaryHeatmap: false,
		});

		let gridColumns = [];

		// Build the phantom trailing column injected in scroll mode. Extracted so the mode-toggle
		// watcher can re-inject / remove it without re-running the full data resync (which was the
		// observed source of the 1-3s lag when switching scroll ↔ fit on a busy grid).
		function buildPhantomColumn(viewportWidth) {
			// 1/3 of the viewport, capped so it never dwarfs a narrow grid (mobile / small panel).
			const phantomWidth = Math.max(150, Math.floor(viewportWidth / 3));
			return {
				id: "__phantom_trailing",
				name: "",
				field: "__phantom_trailing",
				width: phantomWidth,
				// minWidth matches width: autosizeColumns() would otherwise shrink the phantom to fit
				// its empty content (formatter returns ""), defeating the whole point of the trailing
				// headroom. minWidth pins the floor.
				minWidth: phantomWidth,
				resizable: false,
				sortable: false,
				focusable: false,
				selectable: false,
				cssClass: "slick-cell-phantom",
				headerCssClass: "slick-header-phantom",
				// Stamp a footer class too — SlickGrid's footer row renders one cell per column by
				// default, so without this the phantom gets a fully-styled (border + background)
				// footer cell at the right end of the grid which spoils the "invisible trailing
				// column" effect.
				footerCssClass: "slick-footer-phantom",
				formatter: () => "",
			};
		}

		// Per-grid memo of column-id → user-visible width. Survives across resyncs (e.g. a Submit
		// adding a new measure) so existing columns keep whatever width the user manually set, and
		// only the newly-introduced columns get an automatic size. Without this memo every resync
		// hit `grid.autosizeColumns()`, which rebalanced every column's width — disorienting when
		// the user just wanted to append a column.
		//
		// Seeded from `preferencesStore.getCubeColumnWidths(...)` so the user's per-cube widths
		// survive page reloads. The mode-aware split lives at the preferences layer (different
		// bucket key per scroll/fit mode):
		//   - scroll: stored value IS the literal width in px.
		//   - fit:    stored value is a RELATIVE WEIGHT (default 1, larger = wider). We convert
		//             back to a working px value here using `WEIGHT_BASE_PX` so SlickGrid's fit-mode
		//             balancer has something to scale (it preserves ratios → any positive base
		//             works; we pick 100 because that's the natural mental model "1 unit weight
		//             = 100 px before viewport-scaling").
		const WEIGHT_BASE_PX = 100;
		const columnWidthMemo = new Map();
		// True while the layout-toggle handler is mutating columns (adding/removing the phantom column,
		// reapplying memo widths, running the scroll-mode autofit). The `onColumnsResized` subscriber
		// returns early under this flag — without it, the intermediate widths emitted by SlickGrid mid-
		// transition would leak into the wrong bucket. Concretely: when toggling fit→scroll, SlickGrid
		// emits onColumnsResized with the prior fit-mode widths *after* preferencesStore.gridLayout has
		// already flipped to "scroll", so the resize subscriber would persist those (tiny) fit widths
		// into the SCROLL bucket. The next fit→scroll transition would then find the SCROLL bucket
		// non-empty, skip the autofit pass, and rehydrate the tiny widths instead.
		let isApplyingLayoutToggle = false;
		// Snapshot of the last full render's footer inputs. SlickGrid's `setColumns(...)` clears the
		// footer row DOM, so any toggle path that calls setColumns (phantom add/remove, width
		// reapplication, autofit) must re-run `gridHelper.updateFooters` afterwards. The layout
		// toggle handler intentionally avoids `resyncData` for perf, so we keep this snapshot
		// instead. Populated at the end of every non-empty `resyncData` pass; consumed by the
		// layout watcher below.
		/** @type {{ columnNames: string[], view: any, measureStats: any, formatOptions: any } | null} */
		let lastFooterSnapshot = null;

		// Count of data columns whose left edge starts past the visible viewport's right edge —
		// i.e. how many columns the user could reveal by scrolling right. Drives a small floating
		// chip overlayed in the top-right of the grid so the user knows the table is wider than
		// what they see. Always 0 in fit mode (no horizontal scroll). Updated on horizontal scroll,
		// column resize, layout toggle, and after every resync.
		const recomputeOffscreenColumns = () => {
			if (!grid) return;
			const setCount = (n) => {
				if (props.gridShared) props.gridShared.offscreenColumnsRight = n;
			};
			if (preferencesStore.gridLayout !== "scroll") {
				setCount(0);
				return;
			}
			const viewport = typeof grid.getViewportNode === "function" ? grid.getViewportNode() : null;
			if (!viewport) {
				setCount(0);
				return;
			}
			const viewportRight = viewport.scrollLeft + viewport.clientWidth;
			let cumulative = 0;
			let count = 0;
			// Skip the frozen rowIndex column (id "id") and the invisible phantom — they aren't
			// part of the "more columns to discover" affordance the chip is meant to advertise.
			for (const col of grid.getColumns()) {
				if (col.id === "__phantom_trailing" || col.id === "id") continue;
				if (cumulative >= viewportRight) count++;
				cumulative += col.width || 0;
			}
			setCount(count);
		};
		const scrollToRightEnd = () => {
			if (!grid || typeof grid.getViewportNode !== "function") return;
			const viewport = grid.getViewportNode();
			if (!viewport) return;
			viewport.scrollLeft = viewport.scrollWidth;
		};
		// TEMP: disabled while we evaluate whether scroll-mode auto-defaults work better without
		// the persisted-widths layer. Flip back to `true` to re-enable hydrating column widths
		// from preferences. When false, the memo stays empty, which means: in scroll mode the
		// fit→scroll autofit (`min(SCROLL_MODE_AUTOFIT_CAP_PX, fitWidth)`) runs on every toggle
		// instead of being short-circuited by a non-empty bucket.
		const HYDRATE_WIDTHS_FROM_PREFS = false;
		const hydrateColumnWidthMemo = () => {
			columnWidthMemo.clear();
			if (!HYDRATE_WIDTHS_FROM_PREFS) return;
			const mode = preferencesStore.gridLayout;
			const persisted = preferencesStore.getCubeColumnWidths(props.endpointId, props.cubeId, mode);
			for (const [colId, v] of Object.entries(persisted || {})) {
				if (typeof v !== "number" || v <= 0) continue;
				const pxWidth = mode === "fit" ? Math.round(v * WEIGHT_BASE_PX) : v;
				columnWidthMemo.set(colId, pxWidth);
			}
		};
		hydrateColumnWidthMemo();

		const rendering = ref(false);

		function renderingDone() {
			rendering.value = false;
			props.tabularView.loading.rendering = false;
			if (props.tabularView.timing.rendering_startedAt) {
				props.tabularView.timing.rendering = Date.now() - +props.tabularView.timing.rendering_startedAt;
				delete props.tabularView.timing.rendering_startedAt;
			} else {
				// another cell already registered renderering as done
			}
		}

		function renderCallback(cellNode, row, dataContext, colDef) {
			// We assume rendering is done as soon as one cell is rendered
			renderingDone();
		}

		const data = reactive({ array: [] });

		const currentSortCol = reactive({ sortId: undefined, sortAsc: true });

		// https://github.com/6pac/SlickGrid/wiki/Providing-data-to-the-grid
		let resyncData = function () {
			const view = props.tabularView.view;

			gridColumns = [];
			// Per-measure stats (min / max / sum / count) computed from `view.values`. Set in
			// the non-empty branch below and threaded into both `measuresToGridColumns` (for
			// the heatmap cell formatter) and `updateFooters` (for the min/sum/max summary).
			// Left undefined when the grid is empty so formatters degrade to plain numbers.
			let measureStats;

			// Null view = cleared state (e.g. right after Reset, when queryModel is empty so no
			// query was fired). Render a blank grid with a single placeholder column and zero
			// data rows — SlickGrid misbehaves with `setColumns([])`, so keep at least one column.
			// Bail out before the code below, which dereferences `view.coordinates` and
			// `tabularView.query.groupBy.columns` (both undefined in this state).
			//
			// SlickGrid's frozen-column + rowSpan combo does NOT redraw on `dataView.refresh()`
			// alone — the previous viewport's rendered rows survive. We must explicitly
			// invalidate the row cache and re-render, mirroring the trailing call in the
			// non-empty branch below.
			if (!view) {
				console.log("Rendering empty view (no query)");
				gridColumns.push({ id: "empty", name: "", field: "empty", sortable: false });
				data.array = [];
				gridMetadata.nb_rows = 0;
				grid.setColumns(gridColumns);
				dataView.beginUpdate();
				dataView.setItems(data.array, "id");
				dataView.endUpdate();
				grid.remapAllColumnsRowSpan();
				grid.invalidate();
				grid.render();
				return;
			}

			// Do not allow sorting until it is compatible with rowSpans
			const sortable = gridHelper.isSortable();

			// We always show the id column
			// It is especially useful on the grandTotal without measure: the idColumn enables the grid not to be empty
			// If the grid was empty, `renderCallback` would not be called-back
			{
				const column = { id: "id", name: "#row", field: "id", width: 5, sortable: sortable, asyncPostRender: renderCallback };
				gridColumns.push(column);
			}

			data.array = [];

			// from rowIndex to columnIndex to span height
			const metadata = {
				// We leave an example rowSpan but it will reset on first dataset
				0: {
					columns: {
						1: { rowspan: 3 },
					},
				},
			};
			// Delete the example
			delete metadata[0];

			// DRILLTHROUGH: the response carries one entry per source row, with per-aggregator (aliased) keys
			// that do not necessarily appear in `query.measures`. The schema (groupBy + measures) is therefore
			// discovered from the rows themselves rather than from the submitted query.
			const isDrillthrough = (props.tabularView.query.options || []).includes("DRILLTHROUGH");

			// May be String or Object (decorating a column with calculated coordinates)
			const rawColumnNames = props.tabularView.query.groupBy.columns;
			let columnNames = rawColumnNames.map((rawC) => {
				if (typeof rawC === "object") {
					return rawC.column;
				} else {
					return rawC;
				}
			});
			if (isDrillthrough) {
				// Union of all coordinate keys across rows, preserving insertion order.
				const seen = new Set(columnNames);
				for (const row of view.coordinates) {
					for (const k of Object.keys(row)) {
						if (!seen.has(k)) {
							seen.add(k);
							columnNames.push(k);
						}
					}
				}
			}

			// In DRILLTHROUGH mode the formatters distinguish two missing-value cases. A column that IS in
			// the user-submitted query (groupBy or measures) renders missing values as `NULL` — the user
			// asked for that column, so the placeholder reads as "the data is null". A column that is NOT
			// in the user query but appeared in the response (because the engine surfaced an underlying
			// aggregator alias from a Combinator, or because the row-inclusion filter added a column to the
			// groupBy) renders missing values as `Out of DT` — outside the user's contract.
			const requestedColumns = new Set();
			if (isDrillthrough) {
				for (const c of rawColumnNames) {
					requestedColumns.add(typeof c === "object" ? c.column : c);
				}
				for (const m of props.tabularView.query.measures || []) {
					// `query.measures` may be an array of strings (`"delta"`) or referenced-measure objects
					// (`{ ref: "delta" }`) depending on the wizard's edit state.
					requestedColumns.add(typeof m === "object" ? m.name || m.ref : m);
				}
			}

			if (view.coordinates.length === 0) {
				// TODO Why do we show an empty column? Maybe to force having something to render
				const column = { id: "empty", name: "empty", field: "empty", sortable: sortable, asyncPostRender: renderCallback };

				gridColumns.push(column);
				data.array.push({ id: "0", empty: "empty" });
				gridMetadata.nb_rows = 0;

				// TODO How to know when the empty grid is rendered? (This may be slow if previous grid was large)
			} else {
				rendering.value = true;
				props.tabularView.loading.rendering = true;
				props.tabularView.timing.rendering_startedAt = new Date();

				// https://stackoverflow.com/questions/1232040/how-do-i-empty-an-array-in-javascript
				console.log(`Rendering columnNames=${columnNames}`);
				gridColumns.push(...gridHelper.groupByToGridColumns(columnNames, props.queryModel, renderCallback, isDrillthrough, requestedColumns));

				// measureNames may be filled on first row if we requested no measure and received the default measure
				let measureNames = props.tabularView.query.measures;
				if (isDrillthrough) {
					// In DRILLTHROUGH the response uses per-aggregator aliases (e.g. `k1`, `k1_1`) which may
					// differ from the user-submitted `measures`. Discover the value schema as the union of
					// all keys appearing in `view.values`, preserving first-seen order.
					const seenMeasures = new Set();
					measureNames = [];
					for (const row of view.values) {
						for (const k of Object.keys(row)) {
							if (!seenMeasures.has(k)) {
								seenMeasures.add(k);
								measureNames.push(k);
							}
						}
					}
				}
				console.log(`Rendering measureNames=${measureNames}`);

				// Compute per-measure min/max/sum stats BEFORE building the column definitions
				// so the cell formatter can paint a heatmap background on each numeric cell, and
				// so the footer row can surface min/sum/max without re-scanning the values.
				measureStats = gridHelper.computeMeasureStats(measureNames, view.values);
				// Secondary-heatmap stats: per-measure min/max bucketed by the parent slice (the row's
				// groupBy values minus the LAST view column). Drives the per-cell vertical bar so each
				// cell can be compared to its sibling rows under the same parent member.
				const parentColumnNames = columnNames.slice(0, -1);
				const parentSliceStats = gridHelper.computeParentSliceStats(measureNames, parentColumnNames, view.coordinates, view.values);
				// Stash the freshly-computed stats on the shared singleton so the per-measure
				// Statistics modal can be opened from the grid header without re-scanning the
				// view. Resetting the visible measure name on each resync prevents stale
				// numbers from being shown if the user re-opens the modal after a fresh query.
				if (measureStatsModel) {
					measureStatsModel.allStats = measureStats;
				}

				// TODO Refresh the columns on `formatOptions` changes, else we need to query to see the format changes
				gridColumns.push(
					...gridHelper.measuresToGridColumns(
						measureNames,
						props.queryModel,
						renderCallback,
						formatOptions,
						measureStats,
						parentSliceStats,
						parentColumnNames,
						isDrillthrough,
						requestedColumns,
					),
				);

				{
					props.tabularView.loading.sorting = true;
					const sortingStart = new Date();
					props.tabularView.timing.sorting_startedAt = sortingStart;
					try {
						gridHelper.sortRows(columnNames, view.coordinates, view.values);
					} finally {
						props.tabularView.loading.sorting = false;
						props.tabularView.timing.sorting = Date.now() - +sortingStart;
						delete props.tabularView.timing.sorting_startedAt;
					}
				}

				props.tabularView.loading.rowSpanning = true;
				const rowSpanningStart = new Date();
				props.tabularView.timing.rowSpanning_startedAt = rowSpanningStart;
				try {
					if (view.coordinates.length >= 1 && !isDrillthrough) {
						// DRILLTHROUGH responses are intentionally heterogeneous (each source row may carry a
						// different subset of columns), so the first-row sanity check would fire spuriously.
						const rowIndex = 0;
						const coordinatesRow = view.coordinates[rowIndex];
						const measuresRow = view.values[rowIndex];

						gridHelper.sanityCheckFirstRow(columnNames, coordinatesRow, measureNames, measuresRow);
					}

					// https://github.com/vuejs/core/issues/13826
					// RangeError: Maximum call stack size exceeded
					data.array = data.array.concat(gridHelper.toData(columnNames, view.coordinates, measureNames, view.values));

					gridHelper.computeRowSpan(columnNames, metadata, view.coordinates);
				} finally {
					props.tabularView.loading.rowSpanning = false;
					props.tabularView.timing.rowSpanning = Date.now() - +rowSpanningStart;
					delete props.tabularView.timing.rowSpanning_startedAt;
				}
			}

			console.debug("rowSpans: ", metadata);

			// Layout mode: in `scroll` mode, append an invisible "phantom" trailing column to provide
			// horizontal headroom past the last real column. This lets the user grab the LAST real
			// column's right resize handle and widen it freely — without the phantom, that handle sits
			// at the canvas edge and can only shrink the column. The phantom is excluded from every
			// downstream concern (measureNames, columnNames, footer aggregations, stats) because we
			// add it to `gridColumns` ONLY, after the helpers have already built their parts.
			const isScrollLayout = preferencesStore.gridLayout === "scroll";
			if (isScrollLayout && gridColumns.length > 0) {
				const containerEl = document.getElementById(props.domId);
				const viewportWidth = containerEl ? containerEl.clientWidth : 600;
				gridColumns.push(buildPhantomColumn(viewportWidth));
			}

			// `memoWasEmptyBefore` distinguishes the FIRST resync (initial load / F5 — every column
			// looks "new") from subsequent ones (only the freshly-added column is new). The two
			// scenarios need different auto-sizing strategies: initial load can't measure the DOM
			// (it isn't rendered yet, our measureFromDom would return 0 and we'd ship 40-px columns),
			// so we fall back to SlickGrid's metadata-based `autosizeColumns()` for the whole grid.
			// Add-column should NEVER touch existing widths — so we do per-column auto-fit of NEW
			// columns only.
			const memoWasEmptyBefore = columnWidthMemo.size === 0;

			// Restore the user's previously-set widths for columns that survived the resync. NEW
			// columns (no memo entry) keep whatever default width came out of the helper builders;
			// the post-setColumns block sizes them appropriately.
			const newColumnIds = [];
			for (const col of gridColumns) {
				if (columnWidthMemo.has(col.id)) {
					col.width = columnWidthMemo.get(col.id);
				} else {
					newColumnIds.push(col.id);
				}
			}

			grid.setColumns(gridColumns);

			if (isScrollLayout && newColumnIds.length > 0) {
				if (memoWasEmptyBefore) {
					// Initial scroll-mode load: SlickGrid's `grid.autosizeColumns()` is a no-op
					// here (its viewport-balancing branch only runs when forceFitColumns is true),
					// so columns would stay at the default ~80 px. Instead, defer one frame so
					// cells are in the DOM and `autoFitColumnWidth` can measure them, then apply
					// `min(SCROLL_MODE_AUTOFIT_CAP_PX, naturalContentWidth)` per column — same
					// strategy as the fit→scroll watcher above.
					requestAnimationFrame(() => {
						if (!grid) return;
						const cols = grid.getColumns();
						const dataCols = cols.filter((c) => c.id !== "__phantom_trailing");
						let dirty = false;
						dataCols.forEach((col, idx) => {
							const natural = autoFitColumnWidth(grid, dataView, col, idx);
							if (natural <= 0) return;
							const capped = Math.min(SCROLL_MODE_AUTOFIT_CAP_PX, natural);
							if (col.width !== capped) {
								col.width = capped;
								dirty = true;
							}
						});
						if (dirty) {
							grid.setColumns(cols);
							if (typeof grid.updateCanvasWidth === "function") grid.updateCanvasWidth(true);
							// `setColumns` clears the footer row DOM — re-populate from the snapshot
							// captured at the end of the synchronous `resyncData` pass above. Without
							// this the footer is empty on a fresh F5 in scroll mode.
							if (lastFooterSnapshot) {
								gridHelper.updateFooters(
									grid,
									lastFooterSnapshot.columnNames,
									lastFooterSnapshot.view.coordinates,
									lastFooterSnapshot.view.values,
									lastFooterSnapshot.measureStats,
									lastFooterSnapshot.formatOptions,
								);
							}
						}
						recomputeOffscreenColumns();
					});
				} else {
					// Subsequent resync (e.g. user added a measure or groupBy column). Per-column
					// auto-fit for the new columns only; existing columns keep their memoed width.
					// `autosizeColumns()` is intentionally NOT used here because it rebalances every
					// column — disorienting when the user just wanted to append one.
					for (const newId of newColumnIds) {
						const newIdx = gridColumns.findIndex((c) => c.id === newId);
						if (newIdx < 0) continue;
						try {
							const w = autoFitColumnWidth(grid, dataView, gridColumns[newIdx], newIdx);
							if (w > 0) {
								gridColumns[newIdx].width = w;
							}
						} catch (e) {
							console.warn("Per-column auto-fit failed for", newId, e);
						}
					}
					// Light update mirroring SlickGrid's internal drag-resize completion path.
					// `updateCanvasWidth(true)` is the key piece — it recomputes the canvas width
					// AND re-applies column widths with the fresh dimensions, which is what makes a
					// column actually shrink visually. Avoids the heavy `setColumns(...)` re-render
					// (1-3 s on a busy grid). See `applyAutoFitWidth` for the same pattern.
					if (typeof grid.applyColumnHeaderWidths === "function") {
						grid.applyColumnHeaderWidths();
					}
					if (typeof grid.updateCanvasWidth === "function") {
						grid.updateCanvasWidth(true);
					} else if (typeof grid.applyColumnWidths === "function") {
						grid.applyColumnWidths();
					}
				}
			}

			// Snapshot every column's current width back into the memo so the next resync restores
			// even the columns we just auto-sized (instead of resizing them again).
			for (const col of gridColumns) {
				columnWidthMemo.set(col.id, col.width);
			}

			gridHelper.updateFooters(grid, columnNames, view.coordinates, view.values, measureStats, formatOptions);
			// Snapshot for the layout-toggle watcher (see `isApplyingLayoutToggle` comment). The
			// `view` reference is the same object the watcher sees via props, but capturing it
			// explicitly insulates us from a later swap mid-toggle.
			lastFooterSnapshot = { columnNames, view, measureStats, formatOptions };
			recomputeOffscreenColumns();

			dataView.getItemMetadata = (row) => {
				return metadata[row] && metadata[row].attributes ? metadata[row] : (metadata[row] = { attributes: { "data-row": row }, ...metadata[row] });
			};

			// https://github.com/6pac/SlickGrid/wiki/DataView#batching-updates
			dataView.beginUpdate();
			dataView.setItems(data.array);
			dataView.endUpdate();

			gridMetadata.nb_rows = data.array.length;

			// https://github.com/6pac/SlickGrid/issues/1114
			grid.remapAllColumnsRowSpan();

			// https://github.com/6pac/SlickGrid/wiki/Slick.Grid#invalidate
			// since we have a rowspan that spans nearly the entire length to the bottom,
			// we need to invalidate everything so that it recalculate all rowspan cell heights
			grid.invalidate();
		};

		// https://stackoverflow.com/questions/12128680/slickgrid-what-is-a-data-view
		dataView = new SlickDataView({});

		dataView.setItems([]);

		// https://github.com/6pac/SlickGrid/wiki/Grid-Options
		// `Partial<GridOption>` so SlickGrid's strict typing accepts our partial overrides — every key
		// here is optional in the underlying interface.
		/** @type {Partial<import("slickgrid").GridOption>} */
		let options = {
			// Do not allow re-ordering until it is compatible with rowSpans
			enableColumnReorder: true,
			enableAutoSizeColumns: true,
			// https://github.com/6pac/SlickGrid/wiki/Auto-Column-Sizing
			// autosizeColsMode: "?"
			//			autoHeight: true,
			fullWidthRows: true,
			// `forceFitColumns` is legacy, and related with `autosizeColsMode`.
			// Controlled by `preferencesStore.gridLayout`: `fit` (default) sums columns to viewport
			// width; `scroll` disables the squeeze, exposes a horizontal scrollbar, and lets
			// `grid.autosizeColumns()` (called per resync in `resyncData`) widen columns to fit
			// their header text. The watcher below propagates live toggles via `grid.setOptions(...)`.
			forceFitColumns: preferencesStore.gridLayout !== "scroll",
			// https://github.com/6pac/SlickGrid/blob/master/examples/example10-async-post-render.html		,
			enableAsyncPostRender: true,
			// rowSpan enables showing a single time each value on given column
			enableCellRowSpan: true,
			// rowspan doesn't render well with 'transform', default is 'top'
			// https://github.com/6pac/SlickGrid/blob/master/examples/example-0032-row-span-many-columns.html
			rowTopOffsetRenderType: "top",

			// https://github.com/6pac/SlickGrid/blob/master/examples/example-footer-totals.html
			createFooterRow: true,
			showFooterRow: true,
			footerRowHeight: 28,

			// `rowIndex` column is frozen
			frozenColumn: 1,
		};

		// Initialize with `-1` to have a nice default value
		const clickedCell = ref({ id: "-1" });

		// Use AutoResizer?
		// https://6pac.github.io/SlickGrid/examples/example15-auto-resize.html

		onMounted(() => {
			// SlickGrid requires the DOM to be ready: `onMounted` is needed
			grid = new SlickGrid("#" + props.domId, dataView, gridColumns, options);

			gridHelper.registerHeaderButtons(grid, props.queryModel);

			dataView.refresh();

			gridHelper.registerEventSubscribers(grid, dataView, currentSortCol, clickedCell);

			// Keep the columnWidthMemo current with any manual resize (drag handle) or programmatic
			// resize (dblclick auto-fit). Without this, the next resync would replay the OLD memoed
			// width and discard the user's manual change. Also persist to the preferences store so
			// the user's per-cube widths survive page reloads.
			if (grid.onColumnsResized && typeof grid.onColumnsResized.subscribe === "function") {
				grid.onColumnsResized.subscribe(() => {
					// Skip persistence while the layout-toggle handler is rewriting columns — those
					// intermediate widths belong to the prior mode (SlickGrid re-emits with the old
					// widths *after* gridLayout has already flipped), so persisting them would write
					// them into the new mode's bucket.
					if (isApplyingLayoutToggle) return;
					const cols = grid.getColumns().filter((c) => c.id !== "__phantom_trailing");
					if (cols.length === 0) return;
					const mode = preferencesStore.gridLayout;

					// In FIT mode persist a WEIGHT (each column's width / mean width). Weights are
					// dimensionless, so reopening the cube on a wider screen scales every column up
					// proportionally and the user's chosen ratio is preserved. In SCROLL mode
					// persist the literal px width.
					const persistedValues = {};
					if (mode === "fit") {
						const totalPx = cols.reduce((s, c) => s + (c.width || 0), 0);
						const meanPx = totalPx / cols.length || 1;
						for (const col of cols) {
							columnWidthMemo.set(col.id, col.width);
							persistedValues[col.id] = (col.width || meanPx) / meanPx;
						}
					} else {
						for (const col of cols) {
							columnWidthMemo.set(col.id, col.width);
							persistedValues[col.id] = col.width;
						}
					}
					if (props.endpointId && props.cubeId) {
						preferencesStore.setCubeColumnWidths(props.endpointId, props.cubeId, mode, persistedValues);
					}
					// User-driven resize may reveal / hide columns at the right edge.
					recomputeOffscreenColumns();
				});
			}

			// Update the "N columns offscreen" chip whenever the user scrolls horizontally. SlickGrid
			// exposes `onScroll` once the grid is constructed.
			if (grid.onScroll && typeof grid.onScroll.subscribe === "function") {
				grid.onScroll.subscribe(() => {
					recomputeOffscreenColumns();
				});
			}
			// Publish the scroll-right action onto the shared model so the parent's chip click
			// can invoke it. Done once after mount; the function captures `grid` by closure so
			// it stays valid for the component's lifetime.
			if (props.gridShared) {
				props.gridShared.scrollToRightEnd = scrollToRightEnd;
			}

			// Register the watch once the grid is mounted and initialized.
			//
			// NOT `{ deep: true }` on purpose. `view` is only ever swapped by reference from
			// `onView` (a fresh server response) or nulled by the empty-query short-circuit;
			// we never patch it in place. Conversely, `resyncData` itself calls `sortRows`
			// which mutates `view.coordinates` / `view.values` IN-PLACE — with deep tracking,
			// that would re-fire this very watcher, causing the grid to render twice per
			// edit (and logging `Rendering measureNames=` twice). Shallow reference watching
			// fires exactly once per server response — which is what the UX needs.
			// When the wizard is hidden/shown, the grid column transitions between col-9 and col-12. SlickGrid's
			// viewport size is cached on construction and on explicit resize events; without an explicit
			// notification it keeps drawing against the old width and the layout looks broken until the user
			// triggers Submit or hits F5. Watch `wizardHidden` and call `resizeCanvas()` so SlickGrid recomputes
			// its viewport against the new column width. `nextTick`-style timing is achieved by running on the
			// reactive change AFTER Vue has applied the col-* class binding.
			const preferencesStoreForResize = usePreferencesStore();
			watch(
				() => preferencesStoreForResize.wizardHidden,
				() => {
					if (grid) {
						grid.resizeCanvas();
					}
				},
			);

			// Layout toggle (fit ↔ scroll): light path that ONLY flips the option, adds/removes
			// the phantom trailing column, and re-applies widths. The previous implementation called
			// `resyncData()` which rebuilds `data.array`, re-feeds the dataView, invalidates every
			// row and re-renders — observed at 1-3 s on a busy grid. The data is unchanged on a
			// mode toggle, so a full resync is wasted work.
			watch(
				() => preferencesStoreForResize.gridLayout,
				(newLayout, oldLayout) => {
					if (!grid) return;
					// Guard the entire body — see `isApplyingLayoutToggle` comment. SlickGrid emits
					// onColumnsResized at multiple points below (setOptions, setColumns); without the
					// guard each emission would persist to the new mode's bucket, polluting it with
					// the prior mode's widths.
					isApplyingLayoutToggle = true;
					try {
						const wantPhantom = newLayout === "scroll";
						grid.setOptions({ forceFitColumns: !wantPhantom });
						const cols = grid.getColumns();
						const hasPhantom = cols.length > 0 && cols[cols.length - 1].id === "__phantom_trailing";
						if (wantPhantom && !hasPhantom) {
							const containerEl = document.getElementById(props.domId);
							const viewportWidth = containerEl ? containerEl.clientWidth : 600;
							cols.push(buildPhantomColumn(viewportWidth));
							grid.setColumns(cols);
						} else if (!wantPhantom && hasPhantom) {
							cols.pop();
							grid.setColumns(cols);
						}
						// Re-hydrate the memo from the bucket that matches the new mode — scroll-mode
						// widths and fit-mode widths/weights live in separate per-cube buckets so the
						// user can have different sizings per mode without one bleeding into the other.
						hydrateColumnWidthMemo();
						// Apply the freshly-hydrated widths to the columns currently in the grid.
						const restored = grid.getColumns();
						let dirty = false;
						for (const col of restored) {
							if (col.id === "__phantom_trailing") continue;
							const w = columnWidthMemo.get(col.id);
							if (typeof w === "number" && w > 0 && col.width !== w) {
								col.width = w;
								dirty = true;
							}
						}
						if (dirty) {
							grid.setColumns(restored);
						}
						// First-time fit→scroll transition with no prior user widths in scroll mode: cap
						// each data column at `min(SCROLL_MODE_AUTOFIT_CAP_PX, fitWidth)`. Reading the
						// CURRENT fit-mode pixel width (rather than recomputing a natural content width)
						// keeps narrow columns at their fitted size and only clamps the wide ones — the
						// user keeps the rough balance they had in fit mode, just without any single
						// column running past 300 px.
						// We intentionally do NOT update columnWidthMemo or persist these widths: the
						// user's "preferred" widths must reflect user interaction only. If the user
						// later resizes a column, the resize subscriber will pick that up and persist
						// normally.
						if (oldLayout === "fit" && newLayout === "scroll" && columnWidthMemo.size === 0) {
							// Measure each column's NATURAL content width via `autoFitColumnWidth`
							// (header text + sampled cell widths from the rendered DOM) and cap at
							// SCROLL_MODE_AUTOFIT_CAP_PX. We must NOT use `col.width` here — in fit
							// mode SlickGrid's `forceFitColumns` distributes the viewport evenly
							// across columns, so every `col.width` is `viewport/N` and carries no
							// content information (all columns end up at the same ~80-90 px in
							// scroll mode, far smaller than they should be).
							const dataCols = grid.getColumns().filter((c) => c.id !== "__phantom_trailing");
							let autofitDirty = false;
							dataCols.forEach((col, idx) => {
								const natural = autoFitColumnWidth(grid, dataView, col, idx);
								if (natural <= 0) return;
								const capped = Math.min(SCROLL_MODE_AUTOFIT_CAP_PX, natural);
								if (col.width !== capped) {
									col.width = capped;
									autofitDirty = true;
								}
							});
							if (autofitDirty) {
								grid.setColumns(grid.getColumns());
							}
						}
						grid.resizeCanvas();
						// `setColumns` clears the footer row DOM, so any non-resync path that touched
						// the columns above must re-populate it. Use the snapshot captured at the end
						// of the most recent `resyncData` pass — the underlying data hasn't changed,
						// only the visual mode has.
						if (lastFooterSnapshot) {
							gridHelper.updateFooters(
								grid,
								lastFooterSnapshot.columnNames,
								lastFooterSnapshot.view.coordinates,
								lastFooterSnapshot.view.values,
								lastFooterSnapshot.measureStats,
								lastFooterSnapshot.formatOptions,
							);
						}
					} finally {
						isApplyingLayoutToggle = false;
					}
					recomputeOffscreenColumns();
				},
			);

			watch(
				() => props.tabularView.view,
				(newView, oldView) => {
					console.debug("Detected grid data change", newView, oldView);
					if (!props.tabularView.loading) {
						props.tabularView.loading = {};
					}
					if (!props.tabularView.timing) {
						props.tabularView.timing = {};
					}
					props.tabularView.loading.preparingGrid = true;
					const startPreparingGrid = new Date();
					props.tabularView.timing.preparingGrid_startedAt = startPreparingGrid;
					try {
						resyncData();
					} finally {
						props.tabularView.loading.preparingGrid = false;
						props.tabularView.timing.preparingGrid = Date.now() - +startPreparingGrid;
						delete props.tabularView.timing.preparingGrid_startedAt;
					}
				},
			);
		});

		// Loading-state helpers. All three read from `props.tabularView.loading` and return a
		// display-layer primitive; they live in `adhoc-query-grid-loading.js` so they can be
		// unit-tested without a DOM. Thin instance wrappers are kept here because Vue templates
		// call methods with no args — the wrappers bind `props.tabularView` once.
		const isLoading = () => isLoadingHelper(props.tabularView);
		const loadingPercent = () => loadingPercentHelper(props.tabularView);
		const loadingMessage = () => loadingMessageHelper(props.tabularView);

		return {
			rendering,
			gridMetadata,
			clickedCell,
			isLoading,
			loadingPercent,
			loadingMessage,

			formatOptions,

			data,
			measureStatsModel,
			isEmptyModel,
			isEmptyView,
		};
	},
	template: /* HTML */ `
		<div>
			<div class="spinner-grow" role="status" v-if="loading">
				<span class="visually-hidden">Loading...</span>
			</div>

			<AdhocCellModal :queryModel="queryModel" :clickedCell="clickedCell" :cube="cube" />
			<AdhocMeasureStatsModal :statsModel="measureStatsModel" :formatOptions="formatOptions" />

			<span style="width:100%;" class="position-relative">
				<div :id="domId" class="vh-75 slickgrid-grid"></div>

				<!--
					Empty-state hints. Two variants depending on which kind of "empty" we
					are looking at:
					  - 'isEmptyModel' (no measures + no columns picked yet): point the
					    user to the wizard panel on the left.
					  - 'isEmptyView'  (query was sent but matched zero rows): the typical
					    cause is an over-constrained filter, so point UP to the filter
					    block above the wizard.
				-->
				<div class="position-absolute top-50 start-50 translate-middle text-center text-muted" v-if="!isLoading() && isEmptyModel">
					<i class="bi bi-arrow-left-circle fs-3"></i>
					<div>Use the wizard to pick columns and measures, then Submit to build a query.</div>
				</div>
				<div class="position-absolute top-50 start-50 translate-middle text-center text-muted" v-else-if="!isLoading() && isEmptyView">
					<i class="bi bi-arrow-up-circle fs-3"></i>
					<div>No rows match the current filter.</div>
					<div class="small">Loosen or clear the filter above the wizard to bring rows back.</div>
				</div>

				<div class="position-absolute top-50 start-50 translate-middle" style="width:100%;" v-if="isLoading()">
					<div
						class="progress"
						role="progressbar"
						aria-label="Animated striped example"
						:aria-valuenow="loadingPercent()"
						aria-valuemin="0"
						aria-valuemax="100"
						v-if="isLoading()"
					>
						<div class="progress-bar progress-bar-striped progress-bar-animated" :style="'width: ' + loadingPercent() + '%'">
							{{loadingMessage()}}
						</div>
					</div>
				</div>
			</span>
			<div hidden>props.tabularView.loading={{tabularView.loading}}</div>
			<AdhocGridTimingsBar :tabularView="tabularView" />
			<AdhocGridControls :dataArray="data.array" :formatOptions="formatOptions" />
		</div>
	`,
};
