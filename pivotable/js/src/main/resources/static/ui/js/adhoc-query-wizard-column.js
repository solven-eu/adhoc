import { ref } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import AdhocQueryWizardColumnFilterModal from "./adhoc-query-wizard-column-filter-modal.js";

import { markMatchingWizard } from "./adhoc-query-wizard-search-helpers.js";

import { useUserStore } from "./store-user.js";

// BEWARE: Should probably push an event to the Modal component so it open itself
import { Modal } from "bootstrap";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocQueryWizardColumnFilterModal,
	},
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
		searchOptions: {
			type: Object,
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

		const loadingCoordinates = ref(false);

		function loadColumnCoordinates() {
			loadingCoordinates.value = true;
			store.loadColumnCoordinatesIfMissing(props.cubeId, props.endpointId, props.column).finally(() => {
				loadingCoordinates.value = false;
			});
		}

		function openFilterModal() {
			// https://stackoverflow.com/questions/11404711/how-can-i-trigger-a-bootstrap-modal-programmatically
			// https://stackoverflow.com/questions/71432924/vuejs-3-and-bootstrap-5-modal-reusable-component-show-programmatically
			// https://getbootstrap.com/docs/5.0/components/modal/#via-javascript
			let columnFilterModal = new Modal(document.getElementById("columnFilterModal_" + props.column), {});
			// https://getbootstrap.com/docs/5.0/components/modal/#show
			columnFilterModal.show();

			console.log("Showing modal for column", props.column);
		}

		// Indicates if there a filter (i.e. not `matchAll`) on given column
		function isFiltered() {
			// TODO Handle `not`
			if (!props.queryModel?.filter?.type) {
				return false;
			} else if (props.queryModel.filter.type === "and" || props.queryModel.filter.type === "or") {
				const filters = props.queryModel.filter.filters;
				return filters.some((filter) => filter.type === "column" && filter.column == props.column);
			} else {
				return false;
			}
		}

		const mark = function (text) {
			return markMatchingWizard(props.searchOptions, text);
		};

		// Bucket the loaded cardinality into a Bootstrap badge colour so the user can read "is this
		// safe to groupBy?" at a glance instead of having to interpret the raw number. Thresholds:
		//   - ≤ 100: green (small dimension — always safe).
		//   - ≤ 10K: yellow (medium — usually fine, but the result grid can be tall).
		//   - > 10K: red (high — likely too large to render as a flat groupBy).
		// Returned as a Bootstrap `text-bg-*` utility so it slots into the existing badge markup.
		// TODO Roadmap: expose a backend endpoint that returns every column's cardinality in one
		// round-trip — the DB can compute the whole set efficiently, and the SPA could then auto-
		// populate the badges on schema load instead of paying one HTTP call per click.
		const cardinalityBadgeClass = function (n) {
			if (typeof n !== "number") return "bg-secondary";
			if (n <= 100) return "text-bg-success";
			if (n <= 10000) return "text-bg-warning";
			return "text-bg-danger";
		};

		// Map the backend-provided type string to a Bootstrap Icon class. Lowercase substring
		// matching so both SQL-flavoured (`varchar`, `bigint`, `timestamp`) and Java-flavoured
		// (`String`, `Long`, `Instant`) type names resolve. The actual type string is kept as a
		// tooltip for power users.
		const typeIcon = function (type) {
			if (!type) return "bi-question";
			const lower = String(type).toLowerCase();
			if (/bool|bit/.test(lower)) return "bi-toggle-on";
			if (/date|time|instant/.test(lower)) return "bi-calendar3";
			// Ordering: check int-family BEFORE floating-point families — `double` should still
			// resolve to the number icon, but `bigint` must not be mistaken for an int.
			if (/double|float|decimal|numeric|number/.test(lower)) return "bi-123";
			if (/int|long|short|byte/.test(lower)) return "bi-hash";
			if (/varchar|char|text|string/.test(lower)) return "bi-fonts";
			return "bi-question";
		};

		return {
			loadColumnCoordinates,
			loadingCoordinates,
			openFilterModal,
			isFiltered,
			mark,
			typeIcon,
			cardinalityBadgeClass,
		};
	},
	template: /* HTML */ `
		<!--
			Layout collapse: when the column is NOT selected, only [toggle] [name] [pause] (and the
			Filter button when an active filter exists on the column) are shown — the secondary
			controls (grandTotal, cardinality, type) are pre-emptively hidden because they are
			only meaningful once the column participates in the query. The Filter button stays
			visible whenever a filter is set on this column, so the user can clear it without
			needing to first re-select the column.
		-->
		<div
			class="form-check form-switch mb-1 d-flex align-items-start gap-2"
			:class="queryModel.disabledColumns &amp;&amp; queryModel.disabledColumns[column] ? 'opacity-50' : ''"
		>
			<input class="form-check-input" type="checkbox" role="switch" :id="'column_' + column" v-model="queryModel.selectedColumns[column]" />
			<label
				class="form-check-label text-wrap flex-grow-1"
				:class="queryModel.disabledColumns &amp;&amp; queryModel.disabledColumns[column] ? 'text-decoration-line-through' : ''"
				:for="'column_' + column"
				v-html="mark(column)"
			></label>
			<!--
				Pause / resume toggle for the column. Same affordance as the filter tree and
				measure list — the column stays in selectedColumns so the wizard pill is
				preserved (one click resumes), but the executor strips it from the submitted
				query so the grid restructures around the active subset.
				Always rendered (even when the column is not yet selected) so the affordance
				is discoverable; the icon is muted until the user picks the column.
			-->
			<button
				type="button"
				class="btn btn-sm btn-link p-0 text-decoration-none"
				:class="queryModel.selectedColumns[column] ? '' : 'opacity-25'"
				:title="!queryModel.selectedColumns[column] ? 'Pick this column first to enable pause' : ((queryModel.disabledColumns &amp;&amp; queryModel.disabledColumns[column]) ? 'Resume this column' : 'Pause this column (keep in model, skip at query time)')"
				:disabled="!queryModel.selectedColumns[column]"
				@click.stop="queryModel.disabledColumns &amp;&amp; (queryModel.disabledColumns[column] = !queryModel.disabledColumns[column])"
			>
				<i :class="(queryModel.disabledColumns &amp;&amp; queryModel.disabledColumns[column]) ? 'bi bi-play-circle' : 'bi bi-pause-circle'"></i>
			</button>
		</div>

		<!--
			Secondary controls row. Always rendered so the Estimate button is reachable BEFORE the
			user picks the column — checking cardinality is precisely how a user decides whether
			groupBy on this column is safe.
			Per-control visibility:
			- Estimate / cardinality badge: ALWAYS shown.
			- Filter button: ALWAYS shown (small, link-style; right-aligned).
			- grandTotal toggle and type icon: only when the column is selected — they are
				meaningless before the column participates in the query.
		-->
		<div class="d-flex align-items-center gap-2 flex-wrap mb-1">
			<!--
				grandTotal: icon-only toggle (bi-asterisk). Title provides the label. Only useful when
				the column participates in the query — hidden when not selected.
			-->
			<div v-if="queryModel.selectedColumns[column]" class="form-check form-switch mb-0" :title="'Include grandTotal (*) row for ' + column">
				<input
					class="form-check-input"
					type="checkbox"
					role="switch"
					:id="'columnWithStar_' + column"
					v-model="queryModel.withStarColumns[column]"
					aria-label="grandTotal"
				/>
				<label class="form-check-label" :for="'columnWithStar_' + column"><i class="bi bi-asterisk small"></i></label>
			</div>

			<!--
				Right-side trailing controls: cardinality estimate, type icon, filter. All sit on the right
				edge (ms-auto on the leftmost group element). The cardinality affordance has two states:
				- Not yet loaded: icon-only Estimate button (bi-bar-chart-fill), same link-style sizing as
					the filter / pause buttons so the row stays visually balanced.
				- Loaded: a colour-coded compact badge (green/yellow/red) so the user sees "safe-to-groupBy?"
					at a glance. Click either to refresh.
			-->
			<button
				v-if="!(typeof columnMeta.estimatedCardinality === 'number')"
				type="button"
				@click="loadColumnCoordinates()"
				class="btn btn-sm btn-link p-0 text-decoration-none ms-auto"
				:title="'Estimate cardinality for ' + column + ' (a high cardinality may produce a very tall grid when grouping)'"
			>
				<span v-if="loadingCoordinates">
					<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>
				</span>
				<span v-else><i class="bi bi-bar-chart-fill text-secondary"></i></span>
			</button>
			<button
				v-else
				type="button"
				@click="loadColumnCoordinates()"
				class="badge rounded-pill border-0 ms-auto"
				:class="cardinalityBadgeClass(columnMeta.estimatedCardinality)"
				:title="'Estimated cardinality: ' + columnMeta.estimatedCardinality + '. Click to refresh.'"
			>
				<!-- https://stackoverflow.com/questions/10599933/convert-long-number-into-abbreviated-string-in-javascript-with-a-special-shortn -->
				{{ Intl.NumberFormat('en-US', { notation: "compact", maximumFractionDigits: 1 }).format(columnMeta.estimatedCardinality)}}
				<span v-if="loadingCoordinates">
					<span class="spinner-grow spinner-grow-sm ms-1" role="status">
						<span class="visually-hidden">Loading...</span>
					</span>
				</span>
			</button>

			<!-- Type icon: shown regardless of selection — knowing the column's type is part of deciding whether to add it. -->
			<small class="text-muted" :title="'type: ' + type"><i :class="typeIcon(type)"></i></small>
			<AdhocQueryWizardColumnFilterModal :queryModel="queryModel" :column="column" :type="type" :endpointId="endpointId" :cubeId="cubeId" />
			<button
				type="button"
				@click="openFilterModal()"
				class="btn btn-sm btn-link p-0 text-decoration-none position-relative"
				:title="'Edit filter on ' + column"
			>
				<i class="bi bi-filter" :class="isFiltered() ? 'text-primary' : 'text-secondary'"></i>
				<span class="position-absolute top-0 start-100 translate-middle p-1 bg-danger border border-light rounded-circle" v-if="isFiltered()">
					<span class="visually-hidden">is filtered</span>
				</span>
			</button>
		</div>
	`,
};
