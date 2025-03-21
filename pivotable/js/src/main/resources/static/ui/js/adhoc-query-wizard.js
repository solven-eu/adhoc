import { computed, reactive, ref, watch, onMounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocEndpointHeader from "./adhoc-endpoint-header.js";
import AdhocCubeHeader from "./adhoc-cube-header.js";

import AdhocMeasure from "./adhoc-measure.js";

import AdhocQueryWizardColumn from "./adhoc-query-wizard-column.js";
import AdhocQueryWizardFilter from "./adhoc-query-wizard-filter.js";

import { useUserStore } from "./store-user.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocMeasure,
		AdhocQueryWizardColumn,
		AdhocQueryWizardFilter,
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
		}),
	},
	setup(props) {
		const store = useAdhocStore();
		const userStore = useUserStore();

		store.loadCubeSchemaIfMissing(props.cubeId, props.endpointId);

		const debugQuery = ref(false);
		const explainQuery = ref(false);

		const autoQuery = ref(true);
		const loading = ref(false);

		const search = ref("");

		// Used for manual input of a JSON
		const queryJsonInput = ref("");

		const filtered = function (input) {
			const filter = {};

			for (const column in input) {
				if (column.includes(search.value) || JSON.stringify(input[column]).includes(search.value)) {
					filter[column] = input[column];
				}
			}

			return filter;
		};

		return {
			search,
			filtered,

			debugQuery,
			explainQuery,

			loading,
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
            Build the query

            <form>
                <input class="form-control mr-sm-2" type="search" placeholder="Search" aria-label="Search" id="search" v-model="search" />

                <AdhocQueryWizardFilter :filter="queryModel.filter" v-if="queryModel.filter" />

                <div class="accordion" id="accordionWizard">
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
                                <span v-if="search">
                                    <span class="text-decoration-line-through"> {{ Object.keys(cube.columns.columnToTypes).length}} </span>&nbsp;
                                    <span> {{ Object.keys(filtered(cube.columns.columnToTypes)).length}} </span> Columns
                                </span>
                                <span v-else> {{ Object.keys(cube.columns.columnToTypes).length}} Columns </span>
                            </button>

                            <div v-if="nbColumnFetching > 0">
                                <div
                                    class="progress"
                                    role="progressbar"
                                    aria-label="Basic example"
                                    :aria-valuenow="Object.keys(cube.columns.columnToTypes).length - nbColumnFetching"
                                    aria-valuemin="0"
                                    :aria-valuemax="Object.keys(cube.columns.columnToTypes).length"
                                >
                                    <!-- https://stackoverflow.com/questions/21716294/how-to-change-max-value-of-bootstrap-progressbar -->
                                    <div
                                        class="progress-bar"
                                        :style="'width: ' + 100 * (Object.keys(cube.columns.columnToTypes).length - nbColumnFetching) / Object.keys(cube.columns.columnToTypes).length + '%'"
                                    >
                                        {{(Object.keys(cube.columns.columnToTypes).length - nbColumnFetching)}} /
                                        {{Object.keys(cube.columns.columnToTypes).length}}
                                    </div>
                                </div>
                            </div>
                        </h2>
                        <div id="wizardColumns" class="accordion-collapse collapse" data-bs-parent="#accordionWizard">
                            <div class="accordion-body vh-50 overflow-scroll">
                                <ul v-for="(type, name) in filtered(cube.columns.columnToTypes)" class="list-group">
                                    <li class="list-group-item ">
                                        <AdhocQueryWizardColumn
                                            :queryModel="queryModel"
                                            :column="name"
                                            :type="type"
                                            :endpointId="endpointId"
                                            :cubeId="cubeId"
                                        />
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
                                data-bs-target="#wizardMeasures"
                                aria-expanded="false"
                                aria-controls="wizardMeasures"
                            >
                                <span v-if="search">
                                    <span class="text-decoration-line-through"> {{ Object.keys(cube.measures).length}} </span>&nbsp;
                                    <span> {{ Object.keys(filtered(cube.measures)).length}} </span> Measures
                                </span>
                                <span v-else> {{ Object.keys(cube.measures).length}} Measures </span>
                            </button>
                        </h2>
                        <div id="wizardMeasures" class="accordion-collapse collapse" data-bs-parent="#accordionWizard">
                            <div class="accordion-body vh-50 overflow-scroll">
                                <ul v-for="(measure, name) in filtered(cube.measures)" class="list-group list-group-flush">
                                    <li class="list-group-item">
                                        <div class="form-check form-switch">
                                            <input
                                                class="form-check-input"
                                                type="checkbox"
                                                role="switch"
                                                :id="'measure_' + name"
                                                v-model="queryModel.selectedMeasures[name]"
                                            />
                                            <label class="form-check-label" :for="'measure_' + name">
                                                <AdhocMeasure :measure="measure" />
                                            </label>
                                        </div>
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
                                Options
                            </button>
                        </h2>
                        <div id="wizardOptions" class="accordion-collapse collapse" data-bs-parent="#accordionWizard">
                            <div class="accordion-body vh-50 overflow-scroll">
                                <div class="form-check form-switch">
                                    <input class="form-check-input" type="checkbox" role="switch" id="debugQuery" v-model="debugQuery" />
                                    <label class="form-check-label" for="debugQuery">debug</label>
                                </div>
                                <div class="form-check form-switch">
                                    <input class="form-check-input" type="checkbox" role="switch" id="explainQuery" v-model="explainQuery" />
                                    <label class="form-check-label" for="explainQuery">explain</label>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </form>
        </div>
    `,
};
