import { inject } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import AdhocQueryWizardOptions from "./adhoc-query-wizard-options.js";

import wizardHelper from "./adhoc-query-wizard-helper.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocQueryWizardOptions,
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

		options: {
			type: Object,
			required: true,
		},

		searchOptions: {
			type: Object,
			required: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbColumnFetching"]),
		...mapState(useAdhocStore, {
			metadata(store) {
				return store.metadata;
			},
		}),
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
                    data-bs-target="#wizardOptions"
                    aria-expanded="false"
                    aria-controls="wizardOptions"
                >
                    {{ Object.keys(metadata.query_options).length}} options &nbsp;
                    <small class="badge text-bg-primary">{{filtered(options).length}}</small>
                </button>
            </h2>
            <div id="wizardOptions" class="accordion-collapse collapse" data-bs-parent="#accordionWizard">
                <div class="accordion-body vh-50 overflow-scroll">
                    <AdhocQueryWizardOptions :queryModel="queryModel" />
                </div>
            </div>
        </div>
    `,
};
