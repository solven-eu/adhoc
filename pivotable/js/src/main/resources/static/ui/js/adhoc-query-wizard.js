import { computed, reactive, ref, watch, onMounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocEndpointHeader from "./adhoc-endpoint-header.js";
import AdhocCubeHeader from "./adhoc-cube-header.js";

import AdhocMeasure from "./adhoc-query-wizard-measure.js";

import AdhocQueryWizardColumn from "./adhoc-query-wizard-column.js";
import AdhocQueryWizardFilter from "./adhoc-query-wizard-filter.js";
import AdhocQueryWizardOptions from "./adhoc-query-wizard-options.js";

import { useUserStore } from "./store-user.js";

// Ordering of columns
import _ from "lodashEs";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocMeasure,
		AdhocQueryWizardColumn,
		AdhocQueryWizardFilter,
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

		const autoQuery = ref(true);

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
		});

		// Used for manual input of a JSON
		const queryJsonInput = ref("");

		const filtered = function (inputsAsObjectOrArray) {
			const filtereditems = [];

			const searchedValue = searchOptions.text;
			const searchedValueLowerCase = searchedValue.toLowerCase();

			for (const inputKey in inputsAsObjectOrArray) {
				let match = false;

				const inputElement = inputsAsObjectOrArray[inputKey];
				// We consider only values, as keys are generic
				// For instance, `name` should not match `name=NiceNick`
				const inputElementAsString = searchOptions.throughJson ? JSON.stringify(Object.values(inputElement)) : "";

				if (inputKey.includes(searchedValue) || inputElementAsString.includes(searchedValue)) {
					match = true;
				}

				if (!match && !searchOptions.caseSensitive) {
					// Retry without case-sensitivity
					if (inputKey.toLowerCase().includes(searchedValueLowerCase) || inputElementAsString.toLowerCase().includes(searchedValueLowerCase)) {
						match = true;
					}
				}

				if (match) {
					if (typeof inputsAsObjectOrArray === Array) {
						filtereditems.push(inputElement);
					} else {
						// inputElement may be an Object or a primitive or a String
						if (typeof inputElement === "object") {
							filtereditems.push({ ...inputElement, ...{ key: inputKey } });
						} else {
							filtereditems.push({ key: inputKey, value: inputElement });
						}
					}
				}
			}

			// Measures has to be sorted by name
			// https://stackoverflow.com/questions/8996963/how-to-perform-case-insensitive-sorting-array-of-string-in-javascript
			return _.sortBy(filtereditems, [(resultItem) => (resultItem.key || resultItem.name).toLowerCase()]);
		};

		return {
			searchOptions,
			filtered,
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
            <form>
                <div>
                    <input class="form-control mr-sm-2" type="search" placeholder="Search" aria-label="Search" id="search" v-model="searchOptions.text" />
                    <small>
                        <div class="form-check form-switch">
                            <input class="form-check-input" type="checkbox" role="switch" id="searchCaseSensitive" v-model="searchOptions.caseSensitive" />
                            <label class="form-check-label" for="searchCaseSensitive">Aa</label>
                        </div>
                    </small>
                    <small>
                        <div class="form-check form-switch">
                            <input class="form-check-input" type="checkbox" role="switch" id="searchJson" v-model="searchOptions.throughJson" />
                            <label class="form-check-label" for="searchJson">JSON</label>
                        </div>
                    </small>
                </div>

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
                                <span v-if="searchOptions.text">
                                    <span class="text-decoration-line-through"> {{ Object.keys(cube.columns.columnToTypes).length}} </span>&nbsp;
                                    <span> {{ Object.keys(filtered(cube.columns.columnToTypes)).length}} </span> columns
                                </span>
                                <span v-else> {{ Object.keys(cube.columns.columnToTypes).length}} columns </span>
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
                            <div class="accordion-body vh-50 overflow-scroll px-0">
                                <ul v-for="(columnToType) in filtered(cube.columns.columnToTypes)" class="list-group">
                                    <li class="list-group-item ">
                                        <AdhocQueryWizardColumn
                                            :queryModel="queryModel"
                                            :column="columnToType.key"
                                            :type="columnToType.value"
                                            :endpointId="endpointId"
                                            :cubeId="cubeId"
                                            :searchOptions="searchOptions"
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
                                <span v-if="searchOptions.text">
                                    <span class="text-decoration-line-through"> {{ Object.keys(cube.measures).length}} </span>&nbsp;
                                    <span> {{ Object.keys(filtered(cube.measures)).length}} </span> measures
                                </span>
                                <span v-else> {{ Object.keys(cube.measures).length}} measures </span>
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
                                <AdhocQueryWizardOptions :queryModel="queryModel" />
                            </div>
                        </div>
                    </div>
                </div>
            </form>
        </div>
    `,
};
