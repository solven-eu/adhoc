import { inject } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import AdhocQueryWizardCustomMarker from "./adhoc-query-wizard-custommarker.js";

import wizardHelper from "./adhoc-query-wizard-helper.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocQueryWizardCustomMarker,
	},
	// https://vuejs.org/guide/components/props.html
	props: {
		cubeId: {
			type: String,
			required: true,
		},
		endpointId: {
			type: String,
			required: true,
		},

		customMarkers: {
			type: Object,
			required: true,
		},

		searchOptions: {
			type: Object,
			required: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, {}),
	},
	setup(props) {
		const filtered = function (arrayOrObject) {
			return wizardHelper.filtered(props.searchOptions, arrayOrObject);
		};
		const queried = function (arrayOrObject) {
			return wizardHelper.queried(arrayOrObject);
		};

		const clearFilters = function () {
			return wizardHelper.clearFilters(props.searchOptions);
		};

		const queryModel = inject("queryModel");

		return {
			filtered,
			queried,
			clearFilters,
			queryModel,
		};
	},
	template: /* HTML */ `
        <div class="accordion-item">
            <h2 class="accordion-header">
                <button
                    class="accordion-button collapsed"
                    type="button"
                    data-bs-toggle="collapse"
                    data-bs-target="#wizardCustoms"
                    aria-expanded="false"
                    aria-controls="wizardCustoms"
                >
                    {{ Object.keys(customMarkers).length}} custom markers
                </button>
            </h2>
            <div id="wizardCustoms" class="accordion-collapse collapse" data-bs-parent="#accordionWizard">
                <div class="accordion-body vh-50 overflow-scroll px-0">
                    <ul v-for="customMarker in customMarkers" class="list-group list-group-flush">
                        <li class="list-group-item">
                            <AdhocQueryWizardCustomMarker :queryModel="queryModel" :customMarker="customMarker" />
                        </li>
                    </ul>
                </div>
            </div>
        </div>
    `,
};
