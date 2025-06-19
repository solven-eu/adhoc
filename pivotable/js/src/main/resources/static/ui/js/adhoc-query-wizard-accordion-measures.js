import { inject } from "vue";

import AdhocMeasure from "./adhoc-query-wizard-measure.js";
import AdhocQueryWizardColumn from "./adhoc-query-wizard-column.js";

import wizardHelper from "./adhoc-query-wizard-helper.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocMeasure,
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

		measures: {
			type: Object,
			required: true,
		},

		searchOptions: {
			type: Object,
			required: true,
		},
	},
	computed: {},
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
	            data-bs-target="#wizardMeasures"
	            aria-expanded="false"
	            aria-controls="wizardMeasures"
	        >
	            <span v-if="searchOptions.text || searchOptions.tags.length > 0">
	                <span class="text-decoration-line-through"> {{ Object.keys(measures).length}} </span>&nbsp;
	                <span> {{ Object.keys(filtered(measures)).length}} </span> measures
	            </span>
	            <span v-else> {{ Object.keys(measures).length}} measures </span>&nbsp;
	            <small class="badge text-bg-primary">{{queried(queryModel.selectedMeasures).length}}</small>
	        </button>
	    </h2>
	    <div id="wizardMeasures" class="accordion-collapse collapse" data-bs-parent="#accordionWizard">
	        <div class="accordion-body vh-50 overflow-scroll px-0">
	            <ul v-for="(measure) in filtered(measures)" class="list-group list-group-flush">
	                <li class="list-group-item">
	                    <div class="form-check form-switch">
	                        <input
	                            class="form-check-input"
	                            type="checkbox"
	                            role="switch"
	                            :id="'measure_' + measure.name"
	                            v-model="queryModel.selectedMeasures[measure.name]"
	                        />
	                        <label class="form-check-label" :for="'measure_' + measure.name">
	                            <AdhocMeasure :measure="measure" :showDetails="searchOptions.throughJson" :searchOptions="searchOptions" />
	                        </label>
	                    </div>
	                </li>
	            </ul>

	            <span v-if="0 === filtered(measures).length">
	                Search options match no column. <button type="button" class="btn btn-secondary" @click="clearFilters">clearFilters</button>
	            </span>
	        </div>
	    </div>
	</div>
    `,
};
