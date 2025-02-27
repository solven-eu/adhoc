import { computed, reactive, ref, watch, onMounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocEntrypointHeader from "./adhoc-entrypoint-header.js";
import AdhocCubeHeader from "./adhoc-cube-header.js";

import AdhocMeasure from "./adhoc-measure.js";

import { useUserStore } from "./store-user.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocMeasure,
	},
	// https://vuejs.org/guide/components/props.html
	props: {
	cubeId: {
		type: String,
		required: true,
	},
	entrypointId: {
		type: String,
		required: true,
	},
	
		queryModel: {
			type: Object,
			required: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbSchemaFetching"]),
		...mapState(useAdhocStore, {
			entrypoint(store) {
				return store.entrypoints[this.entrypointId] || { error: "not_loaded" };
			},
			schema(store) {
				return store.schemas[this.entrypointId] || { error: "not_loaded" };
			},
			cube(store) {
				return store.schemas[this.entrypointId]?.cubes[this.cubeId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();
		const userStore = useUserStore();

		store.loadCubeSchemaIfMissing(props.cubeId, props.entrypointId);

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
        <div v-if="(!entrypoint || !cube)">
            <div v-if="(nbSchemaFetching > 0 || nbContestFetching > 0)">
                <div class="spinner-border" role="status">
                    <span class="visually-hidden">Loading cubeId={{cubeId}}</span>
                </div>
            </div>
            <div v-else>
                <span>Issue loading cubeId={{cubeId}}</span>
            </div>
        </div>
        <div v-else-if="entrypoint.error || cube.error">{{entrypoint.error || cube.error}}</div>
        <div v-else>
			Build the query
			
			<form>

				Search: <input class="form-control mr-sm-2" type="search" placeholder="Search" aria-label="Search" id="search" v-model="search">
			
				Columns
				<ul v-for="(type, name) in filtered(cube.columns.columnToTypes)" class="list-group list-group-flush">
				    <li class="list-group-item  d-flex justify-content-between align-items-center">
						<div class="form-check form-switch">
						  <input class="form-check-input" type="checkbox" role="switch" :id="'column_' + name" v-model="queryModel.selectedColumns[name]">
						  <label class="form-check-label" :for="'column_' + name">{{name}}: {{type}}</label>
						</div>
						  <span class="badge bg-primary rounded-pill">?</span>
					</li>
				</ul>
	
				Measures 
				<ul v-for="(measure, name) in filtered(cube.measures)" class="list-group list-group-flush">
				    <li class="list-group-item">
						<div class="form-check form-switch">
						  <input class="form-check-input" type="checkbox" role="switch" :id="'measure_' + name" v-model="queryModel.selectedMeasures[name]">
						  <label class="form-check-label" :for="'measure_' + name">
							  <AdhocMeasure :measure='measure' />
						  </label>
						</div>
				    </li>
				</ul>

				<div class="form-check form-switch">
				  <input class="form-check-input" type="checkbox" role="switch" id="debugQuery" v-model="debugQuery">
				  <label class="form-check-label" for="debugQuery">debug</label>
				</div>
				  <div class="form-check form-switch">
				    <input class="form-check-input" type="checkbox" role="switch" id="explainQuery" v-model="explainQuery">
				    <label class="form-check-label" for="explainQuery">explain</label>
				  </div>
			</form>
        </div>
    `,
};
