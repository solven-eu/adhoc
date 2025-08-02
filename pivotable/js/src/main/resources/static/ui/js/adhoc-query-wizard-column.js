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

		return {
			loadColumnCoordinates,
			loadingCoordinates,
			openFilterModal,
			isFiltered,
			mark,
		};
	},
	template: /* HTML */ `
        <div class="form-check form-switch">
            <input class="form-check-input" type="checkbox" role="switch" :id="'column_' + column" v-model="queryModel.selectedColumns[column]" />
            <label class="form-check-label  text-wrap" :for="'column_' + column" v-html="mark(column)"></label>
        </div>

        <small>{{type}}</small>

        <small>
            <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" role="switch" :id="'columnWithStar_' + column" v-model="queryModel.withStarColumns[column]" />
                <label class="form-check-label  text-wrap" :for="'columnWithStar_' + column">grandTotal</label>
            </div>
        </small>

        <button type="button" @click="loadColumnCoordinates()" class="badge bg-secondary rounded-pill">
            <span v-if="!(typeof columnMeta.estimatedCardinality === 'number')"> ? </span>
            <!-- https://stackoverflow.com/questions/10599933/convert-long-number-into-abbreviated-string-in-javascript-with-a-special-shortn -->
            <span v-else> {{ Intl.NumberFormat('en-US', { notation: "compact", maximumFractionDigits: 1 }).format(columnMeta.estimatedCardinality)}} </span>
            <span v-if="loadingCoordinates">
                <div class="spinner-grow" role="status">
                    <span class="visually-hidden">Loading...</span>
                </div>
            </span>
        </button>

        <AdhocQueryWizardColumnFilterModal :queryModel="queryModel" :column="column" :type="type" :endpointId="endpointId" :cubeId="cubeId" />
        <button type="button" @click="openFilterModal()" class="btn btn-outline-primary position-relative">
            <i class="bi bi-filter"></i>
            <span class="position-absolute top-0 start-100 translate-middle p-2 bg-danger border border-light rounded-circle" v-if="isFiltered()">
                <span class="visually-hidden">is filtered</span>
            </span>
        </button>
    `,
};
