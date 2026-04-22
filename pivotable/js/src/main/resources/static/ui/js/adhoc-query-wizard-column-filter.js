import { computed, ref, watch } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {},
	props: {
		cubeId: {
			type: String,
			required: true,
		},
		endpointId: {
			type: String,
			required: true,
		},

		queryModel: {
			type: Object,
			required: true,
		},

		column: {
			type: String,
			required: true,
		},
		type: {
			type: String,
			required: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbSchemaFetching"]),
		...mapState(useAdhocStore, {
			endpoint(store) {
				return store.endpoints[this.endpointId] || { error: "not_loaded" };
			},
			schema(store) {
				return store.schemas[this.endpointId] || { error: "not_loaded" };
			},
			cube(store) {
				return store.schemas[this.endpointId]?.cubes[this.cubeId] || { error: "not_loaded" };
			},
			columnMeta(store) {
				const columnId = `${this.endpointId}-${this.cubeId}-${this.column}`;
				return store.columns[columnId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadCubeSchemaIfMissing(props.cubeId, props.endpointId);

		// Do not load here, else all columns would be loaded when loading the wizard even if columns are not visible
		// store.loadColumnCoordinatesIfMissing(props.cubeId, props.endpointId, props.column);

		const filterTypes = ["equals", "not_equals", "like", "json"];
		const filterType = ref("no_filter");

		const equalsValue = ref("");
		const likePattern = ref("");

		const rawFilterAsJson = ref("");

		const pendingChanges = ref(false);

		// Suggest-dropdown state for the equals / not_equals combobox.
		const showCoordinatesDropdown = ref(false);
		const highlightedIdx = ref(-1);

		const columnMeta = computed(() => {
			const columnId = `${props.endpointId}-${props.cubeId}-${props.column}`;
			return store.columns[columnId] || { error: "not_loaded" };
		});

		// Local, case-insensitive filtering of the coordinates list by what the user has
		// typed in the combobox input. Capped at 50 rows so long lists stay responsive —
		// the user always has the raw input as an escape hatch if the exact value isn't in
		// the first 50 matches.
		const filteredCoordinates = computed(() => {
			const all = columnMeta.value.coordinates;
			if (!all) return [];
			const sorted = Array.from(all).sort();
			const needle = String(equalsValue.value ?? "").toLowerCase();
			if (!needle) return sorted.slice(0, 50);
			return sorted.filter((c) => String(c).toLowerCase().includes(needle)).slice(0, 50);
		});

		// Walk the top-level AND filter looking for an existing {type:'column', column: target}
		// entry — the shape produced by saveFilter() below. Deeper trees (nested AND/OR, NOT
		// wrappers built elsewhere) are intentionally ignored here: this modal only knows how
		// to round-trip the structure it writes itself.
		function findColumnFilter(filter, target) {
			if (!filter || filter.type !== "and" || !Array.isArray(filter.filters)) return null;
			return filter.filters.find((f) => f && f.type === "column" && f.column === target) || null;
		}

		// Decode an existing column filter into the local form state, so that reopening the
		// modal on a column that already has a filter shows/edits the current value instead
		// of starting blank.
		function loadFromColumnFilter(existing) {
			if (!existing) return;
			const vm = existing.valueMatcher;
			if (vm === null || vm === undefined || typeof vm !== "object") {
				filterType.value = "equals";
				equalsValue.value = vm ?? "";
			} else if (vm.type === "not") {
				filterType.value = "not_equals";
				equalsValue.value = vm.negated ?? "";
			} else if (vm.type === "like") {
				filterType.value = "like";
				likePattern.value = vm.pattern ?? "";
			} else {
				filterType.value = "json";
				rawFilterAsJson.value = JSON.stringify(existing, null, 2);
			}
		}

		// Reset local state whenever the column prop changes — critical for the SINGLETON
		// modal mounted by `adhoc-query-wizard-column-filter-modal-singleton.js` (triggered
		// from the grid column header). That modal reuses ONE child filter component across
		// every column the user clicks, so without this reset the previous column's
		// `equalsValue` would bleed into the new column's modal (user-reported bug). The
		// PER-COLUMN modal (`adhoc-query-wizard-column-filter-modal.js`) does not hit this
		// path because each column gets its own child instance, but the reset is harmless
		// there too. Also preloads any already-saved filter for the new column so the modal
		// shows/edits current state rather than starting from scratch.
		let isResetting = false;
		function resetForCurrentColumn() {
			isResetting = true;
			try {
				filterType.value = "no_filter";
				equalsValue.value = "";
				likePattern.value = "";
				rawFilterAsJson.value = "";
				showCoordinatesDropdown.value = false;
				highlightedIdx.value = -1;
				pendingChanges.value = false;

				loadFromColumnFilter(findColumnFilter(props.queryModel?.filter, props.column));
			} finally {
				isResetting = false;
			}
		}

		// `immediate: true` so the initial mount sets up state too (important for the
		// singleton path where `setup()` runs once and column changes are prop updates).
		watch(() => props.column, resetForCurrentColumn, { immediate: true });

		watch(filterType, () => {
			if (!isResetting) pendingChanges.value = true;

			// The User selected `equals` filter: ensure we have a subset of coordinate to help him making his filter
			if ((filterType.value === "equals" || filterType.value === "not_equals") && columnMeta.value.error === "not_loaded") {
				store.loadColumnCoordinatesIfMissing(props.cubeId, props.endpointId, props.column);
			}
		});
		watch(equalsValue, () => {
			if (!isResetting) pendingChanges.value = true;
		});
		watch(likePattern, () => {
			if (!isResetting) pendingChanges.value = true;
		});
		watch(rawFilterAsJson, () => {
			if (!isResetting) pendingChanges.value = true;
		});

		// Combobox handlers — the dropdown is a custom overlay rather than `<datalist>`
		// because datalist doesn't filter-as-you-type and its styling is owned by the
		// browser (no way to align it with Bootstrap).
		function onComboInput() {
			showCoordinatesDropdown.value = true;
			highlightedIdx.value = -1;
		}
		function onComboFocus() {
			showCoordinatesDropdown.value = true;
		}
		function onComboBlur() {
			// Delay the hide so a click on a dropdown item registers before we tear it down.
			// `@mousedown.prevent` on the items keeps focus on the input during the click,
			// but Safari still emits blur on mouse-down in some cases, so a short timeout is
			// the pragmatic fix.
			setTimeout(() => {
				showCoordinatesDropdown.value = false;
			}, 150);
		}
		function onComboKeyDown(e) {
			const items = filteredCoordinates.value;
			if (e.key === "ArrowDown") {
				showCoordinatesDropdown.value = true;
				highlightedIdx.value = Math.min(highlightedIdx.value + 1, items.length - 1);
				e.preventDefault();
			} else if (e.key === "ArrowUp") {
				highlightedIdx.value = Math.max(highlightedIdx.value - 1, -1);
				e.preventDefault();
			} else if (e.key === "Enter") {
				if (highlightedIdx.value >= 0 && items[highlightedIdx.value] !== undefined) {
					equalsValue.value = items[highlightedIdx.value];
					showCoordinatesDropdown.value = false;
					e.preventDefault();
				}
			} else if (e.key === "Escape") {
				showCoordinatesDropdown.value = false;
			}
		}
		function selectCoordinate(coord) {
			equalsValue.value = coord;
			showCoordinatesDropdown.value = false;
		}

		function saveFilter() {
			//			{
			//			  "type" : "and",
			//			  "filters" : [ {
			//			    "type" : "column",
			//			    "column" : "a",
			//			    "valueMatcher" : "a1",
			//			    "nullIfAbsent" : true
			//			  }, {
			//			    "type" : "column",
			//			    "column" : "b",
			//			    "valueMatcher" : "b2",
			//			    "nullIfAbsent" : true
			//			  } ]
			//			}
			if (!props.queryModel.filter || !props.queryModel.filter.type) {
				props.queryModel.filter = {};
				props.queryModel.filter.type = "and";
				props.queryModel.filter.filters = [];
			} else if (props.queryModel.filter.type !== "and") {
				throw new Error("We support only 'and'");
			}

			// https://stackoverflow.com/questions/15995963/remove-array-element-on-condition
			props.queryModel.filter.filters = props.queryModel.filter.filters.filter((item) => item.column !== props.column);

			if (filterType.value == "no_filter") {
				console.log("No filter on", props.column);

				// Collapse the wrapping AND to `{}` (matchAll) when stripping this column's
				// filter left no siblings — otherwise the wizard would render an "empty AND"
				// pill, which is ugly and redundant with the matchAll state.
				if (props.queryModel.filter.filters.length === 0) {
					props.queryModel.filter = {};
				}
			} else if (filterType.value == "equals") {
				console.log("filter", props.column, "equals", equalsValue.value);
				const columnFilter = { type: "column", column: props.column, valueMatcher: equalsValue.value };
				props.queryModel.filter.filters.push(columnFilter);
				rawFilterAsJson.value = JSON.stringify(columnFilter);
			} else if (filterType.value == "not_equals") {
				console.log("filter", props.column, "not_equals", equalsValue.value);
				const columnFilter = {
					type: "column",
					column: props.column,
					valueMatcher: {
						type: "not",
						negated: equalsValue.value,
					},
				};
				props.queryModel.filter.filters.push(columnFilter);
				rawFilterAsJson.value = JSON.stringify(columnFilter);
			} else if (filterType.value == "like") {
				console.log("filter", props.column, "like", likePattern.value);
				const columnFilter = {
					type: "column",
					column: props.column,
					valueMatcher: {
						type: "like",
						pattern: likePattern.value,
					},
				};
				props.queryModel.filter.filters.push(columnFilter);
				rawFilterAsJson.value = JSON.stringify(columnFilter);
			} else {
				props.queryModel.filter.filters.push(JSON.parse(rawFilterAsJson.value));
			}

			pendingChanges.value = false;
		}

		return {
			filterTypes,
			filterType,

			equalsValue,
			likePattern,

			rawFilterAsJson,

			pendingChanges,
			saveFilter,

			filteredCoordinates,
			showCoordinatesDropdown,
			highlightedIdx,
			onComboInput,
			onComboFocus,
			onComboBlur,
			onComboKeyDown,
			selectCoordinate,
		};
	},
	template: /* HTML */ `
		<div>
			<div class="mb-2">
				<label class="form-label small text-muted mb-1">Filtering column <strong>{{column}}</strong></label>
				<select class="form-select form-select-sm" aria-label="Filter type" v-model="filterType">
					<option value="no_filter">No filtering</option>
					<option v-for="value in filterTypes" :value="value">{{value}}</option>
				</select>
			</div>

			<div v-if="filterType == 'no_filter'" class="small text-muted">No filter applied on this column.</div>

			<!--
				Combobox for equals / not_equals: a single freetext input with a filter-as-you-type
				dropdown of existing coordinates. Values outside the suggestion list are accepted
				(the user can type anything) — the dropdown is purely assistive. Shared template for
				both operators since the editor is the same; the negation is applied at save time
				via the filterType switch.
			-->
			<div v-else-if="filterType == 'equals' || filterType == 'not_equals'" class="position-relative">
				<input
					type="text"
					class="form-control form-control-sm"
					v-model="equalsValue"
					@input="onComboInput"
					@focus="onComboFocus"
					@blur="onComboBlur"
					@keydown="onComboKeyDown"
					placeholder="Type a value — suggestions appear below"
					aria-label="Filter value"
					autocomplete="off"
				/>
				<ul
					v-if="showCoordinatesDropdown && filteredCoordinates.length"
					class="list-group position-absolute w-100 shadow-sm mt-1"
					style="max-height: 240px; overflow-y: auto; z-index: 1056;"
				>
					<li
						v-for="(coord, i) in filteredCoordinates"
						:key="coord"
						class="list-group-item list-group-item-action small py-1"
						:class="{active: i === highlightedIdx}"
						style="cursor: pointer;"
						@mousedown.prevent="selectCoordinate(coord)"
					>
						{{coord}}
					</li>
				</ul>
				<small v-if="columnMeta.error === 'not_loaded'" class="text-muted d-block mt-1">Loading suggestions…</small>
				<small v-else-if="!columnMeta.coordinates || !columnMeta.coordinates.length" class="text-muted d-block mt-1">
					No suggestions available — type freely.
				</small>
			</div>

			<div v-else-if="filterType == 'like'">
				<input
					type="text"
					class="form-control form-control-sm"
					v-model="likePattern"
					placeholder="Like pattern (e.g. 'FRA%')"
					aria-label="Like pattern"
				/>
			</div>
			<div v-else>
				<textarea
					class="form-control form-control-sm"
					rows="4"
					v-model="rawFilterAsJson"
					placeholder="raw filter as json"
					aria-label="Raw filter JSON"
				></textarea>
			</div>

			<small v-if="pendingChanges" class="text-warning d-block mt-2">Unsaved changes</small>
		</div>
	`,
};
