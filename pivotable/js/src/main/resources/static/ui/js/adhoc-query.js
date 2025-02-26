import { computed, reactive, ref, watch, onMounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocEntrypointHeader from "./adhoc-entrypoint-header.js";
import AdhocCubeHeader from "./adhoc-cube-header.js";

import AdhocMeasure from "./adhoc-measure.js";

import { useUserStore } from "./store-user.js";

import AdhocQueryView from "./adhoc-query-view.js";

// https://stackoverflow.com/questions/7616461/generate-a-hash-from-string-in-javascript
String.prototype.hashCode = function () {
	var hash = 0,
		i,
		chr;
	if (this.length === 0) return hash;
	for (i = 0; i < this.length; i++) {
		chr = this.charCodeAt(i);
		hash = (hash << 5) - hash + chr;
		hash |= 0; // Convert to 32bit integer
	}
	return hash;
};

// Duplicated from store.js
// TODO How can we share such a class?
class NetworkError extends Error {
	constructor(message, url, response) {
		super(message);
		this.name = this.constructor.name;

		this.url = url;
		this.response = response;
	}
}

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocEntrypointHeader,
		AdhocCubeHeader,
		AdhocMeasure,
		AdhocQueryView,
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
		showEntrypoint: {
			type: Boolean,
			default: true,
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

		const selectedColumns = reactive({});
		const selectedMeasures = reactive({});

		const queryJson = computed(() => {
			const columns = Object.keys(selectedColumns).filter((column) => selectedColumns[column] === true);
			const measures = Object.keys(selectedMeasures).filter((measure) => selectedMeasures[measure] === true);

			return { groupBy: { columns: columns }, measureRefs: measures, debug: debugQuery.value, explain: explainQuery.value };
		});

		// Used for manual input of a JSON
		const queryJsonInput = ref("");

		const tabularView = reactive({});

		const sendMoveError = ref("");
		function sendMove() {
			let move = {};

			move.entrypointId = props.entrypointId;
			move.cube = props.cubeId;
			move.query = queryJson.value;
			//			try {
			//				move = JSON.parse(this.queryJson);
			//			} catch (e) {
			//				console.error("Issue parsing json: ", e);
			//				sendMoveError.value = e.message;
			//				return;
			//			}

			async function postFromUrl(url) {
				try {
					loading.value = true;
					const stringifiedQuery = JSON.stringify(move);

					// console.log("Submitting move", move);
					// console.log("Submitting move", stringifiedQuery);

					if (!store.queries["" + stringifiedQuery.hashCode()]) {
						store.queries["" + stringifiedQuery.hashCode()] = {};
					}
					store.queries["" + stringifiedQuery.hashCode()].query = move;

					const fetchOptions = {
						method: "POST",
						headers: { "Content-Type": "application/json" },
						body: stringifiedQuery,
					};
					const response = await userStore.authenticatedFetch(url, fetchOptions);
					if (!response.ok) {
						throw new NetworkError("POST has failed (" + response.statusText + " - " + response.status + ")", url, response);
					}

					const responseTabularView = await response.json();

					// The submitted move may have impacted the leaderboard
					store.$patch((state) => {
						store.queries["" + stringifiedQuery.hashCode()].result = responseTabularView;
						//state.contests[contestId].stale = true;
					});
					sendMoveError.value = "";

					// console.log(responseTabularView);
					tabularView.value = responseTabularView;

					// TODO Rely on a named route and params
					// router.push({ name: "board" });
				} catch (e) {
					console.error("Issue on Network:", e);
					sendMoveError.value = e.message;
				} finally {
					loading.value = false;
				}
			}

			return postFromUrl(`/cubes/query`);
		}

		const queryResult = computed(() => {
			return { selectedColumns: selectedColumns, selectedMeasures: selectedMeasures };
		});

		const filtered = function (input) {
			const filter = {};

			for (const column in input) {
				if (column.includes(search.value) || JSON.stringify(input[column]).includes(search.value)) {
					filter[column] = input[column];
				}
			}

			return filter;
		};

		watch(
			() => queryJson.value,
			() => {
				if (autoQuery.value) {
					sendMove();
				}
			},
		);

		// SlickGrid requires a cssSelector
		const domId = ref("slickgrid_" + Math.floor(Math.random() * 1024));

		return {
			queryJson,

			search,
			selectedColumns,
			selectedMeasures,
			filtered,

			sendMove,
			sendMoveError,

			debugQuery,
			explainQuery,
			autoQuery,

			tabularView,
			loading,
			domId,
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
            <AdhocCubeHeader :entrypointId="entrypointId" :cubeId="cubeId" />

			Build the query
			
			<form>

				Search: <input class="form-control mr-sm-2" type="search" placeholder="Search" aria-label="Search" id="search" v-model="search">
			
				Columns
				<ul v-for="(type, name) in filtered(cube.columns.columnToTypes)">
				    <li>
						<div class="form-check form-switch">
						  <input class="form-check-input" type="checkbox" role="switch" :id="'column_' + name" v-model="selectedColumns[name]">
						  <label class="form-check-label" :for="'column_' + name">{{name}}: {{type}}</label>
						</div>
					</li>
				</ul>
	
				Measures 
				<ul v-for="(measure, name) in filtered(cube.measures)">
				    <li>
						<div class="form-check form-switch">
						  <input class="form-check-input" type="checkbox" role="switch" :id="'measure_' + name" v-model="selectedMeasures[name]">
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

			<div>
			    <pre style="height: 10pc; overflow-y: scroll;" class="border text-start">{{queryJson}}</pre>
			</div>

			<!-- Move Submitter-->
			<span>
				<div>
				    <button type="button" @click="sendMove()" class="btn btn-outline-primary">Submit
					</button>
				    <span v-if="sendMoveError" class="alert alert-warning" role="alert">{{sendMoveError}}</span>
				</div>

				<div class="form-check form-switch">
				  <input class="form-check-input" type="checkbox" role="switch" id="autoQuery" v-model="autoQuery">
				  <label class="form-check-label" for="autoQuery">autoQuery</label>
				</div>
			</span>
			loading = {{loading}}
			<AdhocQueryView :tabularView="tabularView" :loading="loading" :domId="domId" />
        </div>
    `,
};
