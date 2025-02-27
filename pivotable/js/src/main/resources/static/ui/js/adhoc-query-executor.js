import { computed, reactive, ref, watch, onMounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocMeasure from "./adhoc-measure.js";

import { useUserStore } from "./store-user.js";

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
		}				,
		tabularView: {
			type: Object,
			required: true,
		}
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

		const autoQuery = ref(true);
		const loading = ref(false);

		if (!props.queryModel.selectedColumns) {
			props.queryModel.selectedColumns = {};
		}
		if (!props.queryModel.selectedMeasures) {
			props.queryModel.selectedMeasures = {};
		}

		const queryJson = computed(() => {
			const columns = Object.keys(props.queryModel.selectedColumns).filter((column) => props.queryModel.selectedColumns[column] === true);
			const measures = Object.keys(props.queryModel.selectedMeasures).filter((measure) => props.queryModel.selectedMeasures[measure] === true);

			return { groupBy: { columns: columns }, measureRefs: measures, debug: props.queryModel.debugQuery?.value, explain: props.queryModel.explainQuery?.value };
		});

		// Used for manual input of a JSON
		const queryJsonInput = ref("");

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
					props.tabularView.value = responseTabularView;

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
			autoQuery,

			sendMove,
			sendMoveError,

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
        </div>
    `,
};
