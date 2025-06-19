import { inject } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import AdhocQueryWizardColumn from "./adhoc-query-wizard-column.js";

import wizardHelper from "./adhoc-query-wizard-helper.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocQueryWizardColumn,
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

		columns: {
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
		...mapState(useAdhocStore, {}),
	},
	setup(props) {
		const queryModel = inject("queryModel");

		const filtered = function (arrayOrObject) {
			return wizardHelper.filtered(props.searchOptions, arrayOrObject, queryModel);
		};
		const queried = function (arrayOrObject) {
			return wizardHelper.queried(arrayOrObject);
		};

		const clearFilters = function () {
			return wizardHelper.clearFilters(props.searchOptions);
		};

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
                    data-bs-target="#wizardColumns"
                    aria-expanded="false"
                    aria-controls="wizardColumns"
                >
                    <span v-if="searchOptions.text || searchOptions.tags.length > 0">
                        <span class="text-decoration-line-through"> {{ Object.keys(columns).length}} </span>&nbsp;
                        <span> {{ Object.keys(filtered(columns)).length}} </span> columns
                    </span>
                    <span v-else> {{ Object.keys(columns).length}} columns </span> &nbsp;
                    <small class="badge text-bg-primary">{{queried(queryModel.selectedColumns).length}}</small>
                </button>

                <div v-if="nbColumnFetching > 0">
                    <div
                        class="progress"
                        role="progressbar"
                        aria-label="Basic example"
                        :aria-valuenow="Object.keys(columns).length - nbColumnFetching"
                        aria-valuemin="0"
                        :aria-valuemax="Object.keys(columns).length"
                    >
                        <!-- https://stackoverflow.com/questions/21716294/how-to-change-max-value-of-bootstrap-progressbar -->
                        <div
                            class="progress-bar"
                            :style="'width: ' + 100 * (Object.keys(columns).length - nbColumnFetching) / Object.keys(columns).length + '%'"
                        >
                            {{(Object.keys(columns).length - nbColumnFetching)}} / {{Object.keys(columns).length}}
                        </div>
                    </div>
                </div>
            </h2>
            <div id="wizardColumns" class="accordion-collapse collapse" data-bs-parent="#accordionWizard">
                <div class="accordion-body vh-50 overflow-scroll px-0">
                    <ul v-for="(columnToType) in filtered(columns)" class="list-group">
                        <li class="list-group-item ">
                            <AdhocQueryWizardColumn
                                :queryModel="queryModel"
                                :column="columnToType.key"
                                :type="columnToType.type"
                                :endpointId="endpointId"
                                :cubeId="cubeId"
                                :searchOptions="searchOptions"
                            />
                        </li>
                    </ul>
                    <span v-if="0 === filtered(columns).length">
                        Search options match no column. <button type="button" class="btn btn-secondary" @click="clearFilters">clearFilters</button>
                    </span>
                </div>
            </div>
        </div>
    `,
};
