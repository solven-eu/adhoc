import queryHelper from "./adhoc-query-helper.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {},
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

		return {
			applyEqualsFilter,
			applyNotEqualsFilter,
			columnIsFilterable,
			getUnderlyingsIfMeasure,
			addMeasure,
			drillthroughThisCell,
			drillthroughThisCellNewTab,
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
						<ul>
							<li v-for="(coordinate, column) in clickedCell">
								{{ column }}: {{ coordinate }}
								<span v-if="columnIsFilterable(column)">
									<button type="button" class="btn" @click="applyEqualsFilter(column, coordinate)">
										<i class="bi bi-filter-circle"></i>
									</button>
									<button type="button" class="btn" @click="applyNotEqualsFilter(column, coordinate)">
										<i class="bi bi-filter-circle-fill"></i>
									</button>
								</span>
								<ul v-if="getUnderlyingsIfMeasure(column)">
									<li v-for="underlying in getUnderlyingsIfMeasure(column)">
										<button type="button" class="btn" @click="addMeasure(underlying)">
											{{underlying}} <i class="bi bi-plus-circle"></i>
										</button>
									</li>
								</ul>
							</li>
						</ul>
					</div>
					<div class="modal-footer">
						<!--
							Single DrillThrough entry-point: a split-button dropdown surfaces the two outcomes
							(in-view vs new-tab) under one menu, so the footer doesn't grow with each new variant
							(future: "DrillThrough on a side panel", "DrillThrough as CSV export", …).
						-->
						<div class="btn-group">
							<button type="button" class="btn btn-outline-secondary dropdown-toggle" data-bs-toggle="dropdown" aria-expanded="false">
								<i class="bi bi-zoom-in"></i> DrillThrough
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
										<i class="bi bi-zoom-in me-1"></i> DrillThrough this view
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
										<i class="bi bi-box-arrow-up-right me-1"></i> DrillThrough in new tab
									</button>
								</li>
							</ul>
						</div>
						<button type="button" class="btn btn-primary" data-bs-dismiss="modal">Ok</button>
					</div>
				</div>
			</div>
		</div>
	`,
};
