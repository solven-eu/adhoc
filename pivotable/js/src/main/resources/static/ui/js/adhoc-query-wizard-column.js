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
		};
	},
	template: /* HTML */ `
		<!--
			Two-row compact layout (down from four).
			Row 1: toggle + name — full row, so long column names wrap cleanly instead of being
			squeezed to one character per line in the narrow col-3 sidebar.
			Row 2: secondary controls (type, grandTotal asterisk toggle, cardinality badge,
			filter button) all on one tight horizontal line.
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

		<div class="d-flex align-items-center gap-2 flex-wrap">
			<!-- Type as a Bootstrap Icon; the full type string remains as a tooltip. -->
			<small class="text-muted" :title="'type: ' + type"><i :class="typeIcon(type)"></i></small>

			<!-- grandTotal: icon-only toggle (bi-asterisk). Title provides the label. -->
			<div class="form-check form-switch mb-0" :title="'Include grandTotal (*) row for ' + column">
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

			<button
				type="button"
				@click="loadColumnCoordinates()"
				class="badge bg-secondary rounded-pill"
				:title="'Click to load column cardinality for ' + column"
			>
				<span v-if="!(typeof columnMeta.estimatedCardinality === 'number')"> ? </span>
				<!-- https://stackoverflow.com/questions/10599933/convert-long-number-into-abbreviated-string-in-javascript-with-a-special-shortn -->
				<span v-else> {{ Intl.NumberFormat('en-US', { notation: "compact", maximumFractionDigits: 1 }).format(columnMeta.estimatedCardinality)}} </span>
				<span v-if="loadingCoordinates">
					<span class="spinner-grow spinner-grow-sm" role="status">
						<span class="visually-hidden">Loading...</span>
					</span>
				</span>
			</button>

			<AdhocQueryWizardColumnFilterModal :queryModel="queryModel" :column="column" :type="type" :endpointId="endpointId" :cubeId="cubeId" />
			<button type="button" @click="openFilterModal()" class="btn btn-outline-primary btn-sm position-relative" :title="'Edit filter on ' + column">
				<i class="bi bi-filter"></i>
				<span class="position-absolute top-0 start-100 translate-middle p-1 bg-danger border border-light rounded-circle" v-if="isFiltered()">
					<span class="visually-hidden">is filtered</span>
				</span>
			</button>
		</div>
	`,
};
