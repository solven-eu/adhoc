import { reactive, ref } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import AdhocMeasure from "./adhoc-query-wizard-measure.js";

import AdhocQueryWizardSearch from "./adhoc-query-wizard-search.js";
import AdhocQueryWizardColumn from "./adhoc-query-wizard-column.js";
import AdhocQueryWizardFilter from "./adhoc-query-wizard-filter.js";
import AdhocQueryWizardCustomMarker from "./adhoc-query-wizard-custommarker.js";
import AdhocQueryWizardOptions from "./adhoc-query-wizard-options.js";

import AdhocQueryWizardMeasureTag from "./adhoc-query-wizard-measure-tag.js";

import AdhocAccordionItemColumns from "./adhoc-query-wizard-accordion-columns.js";

import AdhocWizardTags from "./adhoc-query-wizard-tags.js";

import { useUserStore } from "./store-user.js";

import wizardHelper from "./adhoc-query-wizard-helper.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocMeasure,
		AdhocQueryWizardSearch,
		AdhocQueryWizardColumn,
		AdhocQueryWizardFilter,
		AdhocQueryWizardCustomMarker,
		AdhocQueryWizardOptions,
		AdhocQueryWizardMeasureTag,
		AdhocAccordionItemColumns,
		AdhocWizardTags,
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

		queryModel: {
			type: Object,
			required: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbSchemaFetching", "nbColumnFetching"]),
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

			// Used for options
			metadata(store) {
				return store.metadata;
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadCubeSchemaIfMissing(props.cubeId, props.endpointId);

		const searchOptions = reactive({
			text: "",

			// By default, not case-sensitive
			// Else, a user not seeing a match may be confused
			// While a user wanting case-sentitive can get more easily he has to click the toggle
			caseSensitive: false,

			// By default, we search along the names and the JSON
			// This is useful to report measures by some of their defintition like som filter
			// It may laos be problematic (e.g. searching a measure would report the measures depending on it)
			throughJson: true,

			// Tags can be focused by being added to this list
			tags: [],
		});

		const filtered = function (arrayOrObject) {
			return wizardHelper.filtered(searchOptions, arrayOrObject);
		};
		const queried = function (arrayOrObject) {
			return wizardHelper.queried(arrayOrObject);
		};

		const removeTag = function (tag) {
			return wizardHelper.removeTag(searchOptions, tag);
		};

		const clearFilters = function () {
			return wizardHelper.clearFilters(searchOptions);
		};

		return {
			searchOptions,
			filtered,
			queried,
			removeTag,
			clearFilters,
		};
	},
	template: /* HTML */ `
        <div v-if="(!endpoint || !cube)">
            <div v-if="(nbSchemaFetching > 0 || nbContestFetching > 0)">
                <div class="spinner-border" role="status">
                    <span class="visually-hidden">Loading cubeId={{cubeId}}</span>
                </div>
            </div>
            <div v-else>
                <span>Issue loading cubeId={{cubeId}}</span>
            </div>
        </div>
        <div v-else-if="endpoint.error || cube.error">{{endpoint.error || cube.error}}</div>
        <div v-else>
            <form class="text-break">
                <AdhocQueryWizardFilter :filter="queryModel.filter" v-if="queryModel.filter" />
                <AdhocQueryWizardSearch :searchOptions="searchOptions" />

                <AdhocWizardTags :cubeId="cubeId" :endpointId="endpointId" :searchOptions="searchOptions" />
                <div class="accordion" id="accordionWizard">
                    <AdhocAccordionItemColumns :cubeId="cubeId" :endpointId="endpointId" :searchOptions="searchOptions" :columns="cube.columns.columns" />
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
                                    <span class="text-decoration-line-through"> {{ Object.keys(cube.measures).length}} </span>&nbsp;
                                    <span> {{ Object.keys(filtered(cube.measures)).length}} </span> measures
                                </span>
                                <span v-else> {{ Object.keys(cube.measures).length}} measures </span>&nbsp;
                                <small class="badge text-bg-primary">{{queried(queryModel.selectedMeasures).length}}</small>
                            </button>
                        </h2>
                        <div id="wizardMeasures" class="accordion-collapse collapse" data-bs-parent="#accordionWizard">
                            <div class="accordion-body vh-50 overflow-scroll px-0">
                                <ul v-for="(measure) in filtered(cube.measures)" class="list-group list-group-flush">
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

                                <span v-if="0 === filtered(cube.measures).length">
                                    Search options match no column. <button type="button" class="btn btn-secondary" @click="clearFilters">clearFilters</button>
                                </span>
                            </div>
                        </div>
                    </div>
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
                                {{ Object.keys(cube.customMarkers).length}} custom markers
                            </button>
                        </h2>
                        <div id="wizardCustoms" class="accordion-collapse collapse" data-bs-parent="#accordionWizard">
                            <div class="accordion-body vh-50 overflow-scroll px-0">
                                <ul v-for="customMarker in cube.customMarkers" class="list-group list-group-flush">
                                    <li class="list-group-item">
                                        <AdhocQueryWizardCustomMarker :queryModel="queryModel" :customMarker="customMarker" />
                                    </li>
                                </ul>
                            </div>
                        </div>
                    </div>
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
                                <small class="badge text-bg-primary">{{filtered(queryModel.options).length}}</small>
                            </button>
                        </h2>
                        <div id="wizardOptions" class="accordion-collapse collapse" data-bs-parent="#accordionWizard">
                            <div class="accordion-body vh-50 overflow-scroll">
                                <AdhocQueryWizardOptions :queryModel="queryModel" />
                            </div>
                        </div>
                    </div>
                </div>
            </form>
        </div>
    `,
};
