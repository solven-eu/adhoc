// @ts-check
import { provide, ref, computed } from "vue";

import AdhocColumnChip from "./adhoc-column-chip.js";
import queryHelper from "./adhoc-query-helper.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: { AdhocColumnChip },
	props: {
		queryModel: {
			type: Object,
			required: true,
		},
		clickedCell: {
			type: Object,
			required: true,
		},
		cube: {
			type: Object,
			required: true,
		},
	},
	computed: {},
	setup(props) {
		// AdhocColumnChip reads its target queryModel via `inject("queryModel")` (so the chip can be dropped
		// anywhere inside the wizard subtree without prop-drilling). The cell-modal lives outside that
		// subtree, so without an explicit provide the chip's dropdown actions (Add as groupBy / Add filter /
		// Edit filter) silently no-op. Forward the same queryModel the cell-modal already operates on so the
		// chip's actions mutate the SAME reactive object — they take effect immediately.
		provide("queryModel", props.queryModel);

		const applyEqualsFilter = function (column, coordinate) {
			// BEWARE This is poor design. We should send some event  managing the queryModel/filters
			if (!props.queryModel.filter || !props.queryModel.filter.type) {
				props.queryModel.filter = {};
				props.queryModel.filter.type = "and";
				props.queryModel.filter.filters = [];
			} else if (props.queryModel.filter.type !== "and") {
				throw new Error("We support only 'and'");
			}

			const columnFilter = { type: "column", column: column, valueMatcher: coordinate };
			props.queryModel.filter.filters.push(columnFilter);
			console.log("Added filter", columnFilter);
			// UX shortcut: equality collapses the column to a constant, so dropping it from the groupBy in
			// the same model edit avoids a single-coordinate header that adds no analytical value. The user
			// is one click away from a re-query — both the filter add and the groupBy drop must land before
			// it fires.
			if (props.queryModel.selectedColumns && props.queryModel.selectedColumns[column]) {
				props.queryModel.selectedColumns[column] = false;
			}
		};
		const applyNotEqualsFilter = function (column, coordinate) {
			// BEWARE This is poor design. We should send some event  managing the queryModel/filters
			if (!props.queryModel.filter || !props.queryModel.filter.type) {
				props.queryModel.filter = {};
				props.queryModel.filter.type = "and";
				props.queryModel.filter.filters = [];
			} else if (props.queryModel.filter.type !== "and") {
				throw new Error("We support only 'and'");
			}

			const columnFilter = {
				type: "column",
				column: column,
				valueMatcher: {
					type: "not",
					negated: coordinate,
				},
			};
			props.queryModel.filter.filters.push(columnFilter);
			console.log("Added filter", columnFilter);
		};

		const columnIsFilterable = function (column) {
			return props.queryModel.selectedColumnsOrdered.includes(column);
		};

		const getUnderlyingsIfMeasure = function (column) {
			if (Object.keys(props.cube.measures).includes(column)) {
				const measure = props.cube.measures[column];

				const underlyings = [];

				if (measure.underlying) {
					underlyings.push(measure.underlying);
				} else if (measure.underlyings) {
					underlyings.push(...measure.underlyings);
				}

				return underlyings;
			} else {
				return [];
			}
		};

		const addMeasure = function (measure) {
			// TODO This should be a toggle
			props.queryModel.selectedMeasures[measure] = true;
		};

		// Build a DRILLTHROUGH for the clicked cell: pin every filterable groupBy coordinate of the
		// clicked row as an additional column-equals filter, then enable the DRILLTHROUGH option so
		// the next Submit returns the underlying rows.
		const drillthroughThisCell = function () {
			Object.entries(props.clickedCell || {}).forEach(([column, coordinate]) => {
				if (columnIsFilterable(column) && coordinate !== undefined && coordinate !== null) {
					applyEqualsFilter(column, coordinate);
				}
			});
			if (!props.queryModel.selectedOptions) {
				props.queryModel.selectedOptions = {};
			}
			props.queryModel.selectedOptions.DRILLTHROUGH = true;
		};

		// Same intent as `drillthroughThisCell`, but emits the result into a fresh tab on the same cube
		// route instead of mutating the current page's queryModel. The target URL carries the (deep-cloned)
		// queryModel + DRILLTHROUGH option in the hash so the new tab restores via the standard
		// `hashToQueryModel` path. Caller's queryModel is left untouched, so the originating tab keeps its
		// state intact.
		const drillthroughThisCellNewTab = function () {
			// Deep-clone to detach from the current queryModel reactive proxy.
			const parsedJson = JSON.parse(JSON.stringify(queryHelper.queryModelToParsedJson(props.queryModel)));

			if (!parsedJson.filter || !parsedJson.filter.type) {
				parsedJson.filter = { type: "and", filters: [] };
			} else if (parsedJson.filter.type !== "and") {
				console.warn("Existing filter is not an AND, cannot append cell coordinates safely", parsedJson.filter);
				return;
			}
			Object.entries(props.clickedCell || {}).forEach(([column, coordinate]) => {
				if (columnIsFilterable(column) && coordinate !== undefined && coordinate !== null) {
					parsedJson.filter.filters.push({ type: "column", column: column, valueMatcher: coordinate });
				}
			});

			if (!parsedJson.options.includes("DRILLTHROUGH")) {
				parsedJson.options.push("DRILLTHROUGH");
			}

			const hash = "#" + encodeURIComponent(JSON.stringify({ query: parsedJson }));
			const url = window.location.pathname + hash;
			window.open(url, "_blank", "noopener");
		};

		/**
		 * Maps a column name to a `true` flag that briefly flips on after the user clicks the row's copy
		 * button — the icon swaps to a green tick + "Copied" label for 1.5 s. Keyed by column so flashing
		 * one row's flag does not disturb the others if the user clicks multiple rows in quick succession.
		 *
		 * @type {import("vue").Ref<Record<string, boolean>>}
		 */
		const copiedFlags = ref({});

		/**
		 * Copy {@code coordinate} to the system clipboard and surface "Copied" feedback on the row keyed by
		 * {@code column}. Mirrors the SQL-copy button in `adhoc-query-plan-mermaid-modal.js`: uses
		 * {@code navigator.clipboard.writeText} (the only API still in spec), no-ops with a console warning
		 * when the page is not in a secure context (`navigator.clipboard` is undefined under plain http).
		 *
		 * <p>{@code coordinate} can be any rendered cell value — string, number, boolean, even {@code null}.
		 * We coerce via {@code String(coordinate)} so the user always gets some textual content; the cell
		 * value's own toString is whatever SlickGrid's formatter rendered, which matches what they see in
		 * the cell.
		 *
		 * @param {string} column the column name (used as the key for the feedback flag)
		 * @param {unknown} coordinate the value to copy
		 */
		const copyCellValue = async (column, coordinate) => {
			try {
				if (!navigator.clipboard || !navigator.clipboard.writeText) {
					console.warn("navigator.clipboard not available (insecure context?) — cell value not copied");
					return;
				}
				await navigator.clipboard.writeText(coordinate == null ? "" : String(coordinate));
				copiedFlags.value = { ...copiedFlags.value, [column]: true };
				// 1.5 s mirrors the SQL-modal feedback. Reset just this key so concurrent flashes on other
				// rows stay independent.
				setTimeout(() => {
					const next = { ...copiedFlags.value };
					delete next[column];
					copiedFlags.value = next;
				}, 1500);
			} catch (e) {
				console.error("Failed copying cell value to clipboard:", e);
			}
		};

		// True when `column` is a measure of the active cube — used to bucket the clickedCell
		// entries into the two tables (coordinates vs measures). Defensive against an empty
		// `cube.measures` (the cube schema may still be loading when the modal opens).
		const isMeasure = function (column) {
			return !!(props.cube && props.cube.measures && Object.prototype.hasOwnProperty.call(props.cube.measures, column));
		};

		// Excel/CSV escaping: wrap a cell in double quotes when it contains a comma, a quote,
		// or a newline; double up embedded quotes per RFC 4180. Cells without those characters
		// are emitted as-is so simple data stays human-readable when pasted into a text editor.
		const csvEscape = function (raw) {
			if (raw === null || raw === undefined) return "";
			const s = String(raw);
			if (/[",\r\n]/.test(s)) {
				return '"' + s.replace(/"/g, '""') + '"';
			}
			return s;
		};

		// Serialise every clickedCell entry as a two-column CSV: header row "Name,Value" then
		// one row per entry, coordinates first then measures (same order as the on-screen
		// tables). Designed for pasting into Excel / Google Sheets: those tools auto-detect
		// the comma delimiter and the RFC 4180 quoting we apply above.
		const copyAllStatus = ref(/** @type {"idle"|"done"|"error"} */ ("idle"));
		const copyAllAsCsv = async () => {
			const lines = ["Name,Value"];
			for (const row of coordinateRows.value) {
				lines.push(csvEscape(row.column) + "," + csvEscape(row.coordinate));
			}
			for (const row of measureRows.value) {
				lines.push(csvEscape(row.column) + "," + csvEscape(row.value));
			}
			const text = lines.join("\n");
			try {
				if (!navigator.clipboard || !window.isSecureContext) {
					console.warn("navigator.clipboard not available — CSV not copied");
					copyAllStatus.value = "error";
					return;
				}
				await navigator.clipboard.writeText(text);
				copyAllStatus.value = "done";
				setTimeout(() => {
					if (copyAllStatus.value === "done") copyAllStatus.value = "idle";
				}, 1500);
			} catch (e) {
				console.error("Failed copying CSV to clipboard:", e);
				copyAllStatus.value = "error";
			}
		};

		// Split `clickedCell` into the two rows the template renders. Computeds rather than
		// inline `v-for` filters so the iteration order is stable (insertion order in the
		// clicked row) and the two lists are easy to assert in tests later.
		const coordinateRows = computed(() => {
			const entries = Object.entries(props.clickedCell || {});
			return entries.filter(([col]) => !isMeasure(col)).map(([column, coordinate]) => ({ column, coordinate }));
		});
		const measureRows = computed(() => {
			const entries = Object.entries(props.clickedCell || {});
			return entries.filter(([col]) => isMeasure(col)).map(([column, value]) => ({ column, value }));
		});

		return {
			applyEqualsFilter,
			applyNotEqualsFilter,
			columnIsFilterable,
			getUnderlyingsIfMeasure,
			addMeasure,
			drillthroughThisCell,
			drillthroughThisCellNewTab,
			copiedFlags,
			copyCellValue,
			coordinateRows,
			measureRows,
			copyAllAsCsv,
			copyAllStatus,
		};
	},
	template: /* HTML */ `
		<div class="modal fade" id="cellModal" tabindex="-1" aria-labelledby="cellModalLabel" aria-hidden="true">
			<div class="modal-dialog modal-dialog-centered">
				<div class="modal-content">
					<div class="modal-header">
						<h5 class="modal-title" id="cellModalLabel">Cell Filter Editor</h5>
						<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
					</div>
					<div class="modal-body">
						<!--
							Two compact 2-column tables: groupBy coordinates on top, measure
							values below. Replaces the previous flat <ul> that mixed both kinds
							of entries on the same line — splitting by kind lets the eye scan
							"what row am I on" (coordinates) separately from "what numbers does
							that row carry" (measures), and gives the action icons a
							predictable column-aligned home.
						-->
						<!--
							Three columns: NAME | VALUE | ACTIONS. The actions cell is anchored to
							the right (text-end + width: 1%) so the icon column lines up vertically
							across rows regardless of how long a coordinate or value gets — a row
							whose value spills onto multiple lines doesn't push its icons sideways.
							Cleaner visual scan than the previous inline-after-value layout where
							icon column-offset drifted per row.
						-->
						<div v-if="coordinateRows.length > 0">
							<h6 class="text-muted small mb-1"><i class="bi bi-funnel me-1"></i>Coordinates</h6>
							<table class="table table-sm table-borderless align-middle mb-3">
								<tbody>
									<tr v-for="row in coordinateRows" :key="'coord-' + row.column">
										<td class="text-nowrap font-monospace text-muted small" style="width: 1%;">
											<AdhocColumnChip :name="row.column" />
										</td>
										<td class="font-monospace">{{ row.coordinate }}</td>
										<td class="text-nowrap text-end" style="width: 1%;">
											<!--
												Order: [filter-in] [filter-out] [copy]. Copy is rendered last so
												it lands at the rightmost position of the cell — aligning
												vertically with the copy-only actions cell in the Measures
												table below. Filter actions: same bi-funnel base for the
												include/exclude pair; exclude adds an overlaid bi-slash-lg to
												read as "filter, but excluded". Primary colour for include,
												muted grey for exclude.
											-->
											<span class="d-inline-flex gap-2 align-items-center">
												<button
													type="button"
													class="btn btn-link p-0 text-primary text-decoration-none"
													:disabled="!columnIsFilterable(row.column)"
													:style="!columnIsFilterable(row.column) ? 'visibility: hidden;' : ''"
													@click="applyEqualsFilter(row.column, row.coordinate)"
													title="Filter on this coordinate (equals)"
												>
													<i class="bi bi-funnel" aria-hidden="true"></i>
												</button>
												<button
													type="button"
													class="btn btn-link p-0 text-muted text-decoration-none position-relative"
													:disabled="!columnIsFilterable(row.column)"
													:style="!columnIsFilterable(row.column) ? 'visibility: hidden;' : ''"
													@click="applyNotEqualsFilter(row.column, row.coordinate)"
													title="Filter out this coordinate (not equals)"
												>
													<i class="bi bi-funnel" aria-hidden="true"></i>
													<i
														class="bi bi-slash-lg position-absolute"
														style="top: 50%; left: 50%; transform: translate(-50%, -50%); font-size: 1.4em;"
														aria-hidden="true"
													></i>
												</button>
												<button
													type="button"
													class="btn btn-link p-0 text-decoration-none"
													@click="copyCellValue(row.column, row.coordinate)"
													:title="copiedFlags[row.column] ? 'Copied!' : 'Copy value to clipboard'"
												>
													<i v-if="copiedFlags[row.column]" class="bi bi-check2 text-success" aria-hidden="true"></i>
													<i v-else class="bi bi-clipboard text-muted" aria-hidden="true"></i>
												</button>
											</span>
										</td>
									</tr>
								</tbody>
							</table>
						</div>

						<div v-if="measureRows.length > 0">
							<h6 class="text-muted small mb-1"><i class="bi bi-rulers me-1"></i>Measures</h6>
							<table class="table table-sm table-borderless align-middle mb-0">
								<tbody>
									<!--
										Per measure: a primary row (name | value | copy action) and, when
										the measure is composite, a secondary row beneath it spanning all
										3 columns and carrying the "+ underlying" pills. Putting the
										underlyings on their own row keeps the primary row clean and the
										actions column narrow and aligned with the coordinates table
										above — the buttons would otherwise widen the actions column
										unpredictably depending on which measures the user clicked.
									-->
									<template v-for="row in measureRows" :key="'measure-' + row.column">
										<tr>
											<!--
												Measure name rendered as plain text rather than via
												AdhocColumnChip: the chip's dropdown surfaces "Add as groupBy" /
												"Add filter" / "Edit filter" — all column-only operations that
												are meaningless for a measure.
											-->
											<td class="text-nowrap font-monospace text-muted small" style="width: 1%;">{{ row.column }}</td>
											<td class="font-monospace">{{ row.value }}</td>
											<td class="text-nowrap text-end" style="width: 1%;">
												<button
													type="button"
													class="btn btn-link p-0 text-decoration-none"
													@click="copyCellValue(row.column, row.value)"
													:title="copiedFlags[row.column] ? 'Copied!' : 'Copy value to clipboard'"
												>
													<i v-if="copiedFlags[row.column]" class="bi bi-check2 text-success" aria-hidden="true"></i>
													<i v-else class="bi bi-clipboard text-muted" aria-hidden="true"></i>
												</button>
											</td>
										</tr>
										<!--
											Underlyings row — only for composite measures. Colspans all 3
											columns so the "+ underlying" pills can flow naturally without
											being squeezed into the narrow actions cell. Subtle muted
											styling + a leading indent makes the row read as a continuation
											of the measure above.
										-->
										<tr v-if="getUnderlyingsIfMeasure(row.column).length > 0">
											<td colspan="3" class="ps-4 pt-0 pb-2 small text-muted border-0">
												<span class="me-2"><i class="bi bi-diagram-2 me-1"></i>underlyings:</span>
												<span class="d-inline-flex flex-wrap gap-1">
													<button
														v-for="underlying in getUnderlyingsIfMeasure(row.column)"
														:key="underlying"
														type="button"
														class="btn btn-sm btn-outline-secondary"
														@click="addMeasure(underlying)"
														:title="'Add measure ' + underlying + ' to the query'"
													>
														<i class="bi bi-plus-circle me-1" aria-hidden="true"></i>{{ underlying }}
													</button>
												</span>
											</td>
										</tr>
									</template>
								</tbody>
							</table>
						</div>
					</div>
					<div class="modal-footer">
						<!--
							Single DrillThrough entry-point: a split-button dropdown surfaces the two outcomes
							(in-view vs new-tab) under one menu, so the footer doesn't grow with each new variant
							(future: "DrillThrough on a side panel", "DrillThrough as CSV export", …).
						-->
						<div class="btn-group">
							<button type="button" class="btn btn-outline-secondary dropdown-toggle" data-bs-toggle="dropdown" aria-expanded="false">
								<i class="bi bi-zoom-in" aria-hidden="true"></i> DrillThrough
							</button>
							<ul class="dropdown-menu shadow-sm">
								<li>
									<button
										type="button"
										class="dropdown-item"
										data-bs-dismiss="modal"
										@click="drillthroughThisCell()"
										title="Pin this cell's coordinates as filters and enable DRILLTHROUGH in this tab; press Submit to fetch the underlying rows."
									>
										<i class="bi bi-zoom-in me-1" aria-hidden="true"></i> DrillThrough this view
									</button>
								</li>
								<li>
									<button
										type="button"
										class="dropdown-item"
										data-bs-dismiss="modal"
										@click="drillthroughThisCellNewTab()"
										title="Open the DRILLTHROUGH for this cell in a new tab — keeps the current view untouched."
									>
										<i class="bi bi-box-arrow-up-right me-1" aria-hidden="true"></i> DrillThrough in new tab
									</button>
								</li>
							</ul>
						</div>
						<!--
							Copy-all CSV: positioned between DrillThrough and Ok so it sits on the
							same row as the other footer actions. Produces a 2-column CSV
							(Name,Value header + one row per coordinate then per measure) suitable
							for pasting into Excel / Google Sheets. RFC 4180 quoting in csvEscape
							so a coordinate carrying a comma or a quote doesn't break the parse.
						-->
						<button
							type="button"
							class="btn btn-outline-secondary"
							@click="copyAllAsCsv"
							:title="copyAllStatus === 'done' ? 'Copied as CSV!' : copyAllStatus === 'error' ? 'Copy failed (clipboard blocked)' : 'Copy all coordinates and measures as a 2-column CSV (paste into Excel)'"
							data-testid="cell-modal-copy-all-csv"
						>
							<i v-if="copyAllStatus === 'done'" class="bi bi-check2 text-success me-1" aria-hidden="true"></i>
							<i v-else-if="copyAllStatus === 'error'" class="bi bi-exclamation-triangle text-danger me-1" aria-hidden="true"></i>
							<i v-else class="bi bi-clipboard-data me-1" aria-hidden="true"></i>
							Copy as CSV
						</button>
						<button type="button" class="btn btn-primary" data-bs-dismiss="modal">Ok</button>
					</div>
				</div>
			</div>
		</div>
	`,
};
