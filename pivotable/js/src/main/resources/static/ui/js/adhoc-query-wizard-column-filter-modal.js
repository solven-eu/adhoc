import { computed, reactive, ref, watch, onMounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import { useUserStore } from "./store-user.js";

import AdhocQueryWizardColumnFilter from "./adhoc-query-wizard-column-filter.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocQueryWizardColumnFilter,
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
		}),
	},
	emits: ["saveFilter"],
	setup(props) {
		const store = useAdhocStore();
		const userStore = useUserStore();

		// https://stackoverflow.com/questions/42632711/how-to-call-function-on-child-component-on-parent-events
		const filterRef = ref(null);

		// Enable saving the filter from the Modal control
		function saveFilter() {
			filterRef.value.saveFilter();
		}

		return {
			filterRef,

			saveFilter,
		};
	},
	template: /* HTML */ `
        <div class="modal fade" :id="'columnFilterModal_' + column" tabindex="-1" aria-labelledby="columnFilterModalLabel" aria-hidden="true">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title" id="columnFilterModalLabel">Filtering column={{column}}</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body">
                        <AdhocQueryWizardColumnFilter
                            :queryModel="queryModel"
                            :column="column"
                            :type="type"
                            :endpointId="endpointId"
                            :cubeId="cubeId"
                            ref="filterRef"
                        />
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-primary" data-bs-dismiss="modal" @click="saveFilter">Ok</button>
                    </div>
                </div>
            </div>
        </div>
    `,
};
