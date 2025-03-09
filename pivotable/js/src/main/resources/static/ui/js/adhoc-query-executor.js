import { computed, reactive, ref, watch, onMounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocMeasure from "./adhoc-measure.js";
import AdhocQueryRawModal from "./adhoc-query-raw-modal.js";

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
		AdhocQueryRawModal,
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
		tabularView: {
			type: Object,
			required: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbSchemaFetching"]),
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
		const loading = ref(false);

		const queryStatus = ref("");

		if (!props.queryModel.selectedColumns) {
			props.queryModel.selectedColumns = {};
		}
		if (!props.queryModel.selectedMeasures) {
			props.queryModel.selectedMeasures = {};
		}

		// This computed property does snapshots of the query
		const queryJson = computed(() => {
			const columns = Object.keys(props.queryModel.selectedColumns).filter((column) => props.queryModel.selectedColumns[column] === true);
			const measures = Object.keys(props.queryModel.selectedMeasures).filter((measure) => props.queryModel.selectedMeasures[measure] === true);

			// https://stackoverflow.com/questions/597588/how-do-you-clone-an-array-of-objects-in-javascript
			// We do a copy as this must not changed when playing with the wizard.
			if (!props.queryModel.selectedColumns2) {
				props.queryModel.selectedColumns2 = [];
			}
			const orderedColumns = props.queryModel.selectedColumns2.slice(0);

			return {
				groupBy: { columns: orderedColumns },
				measures: measures,
				debug: props.queryModel.debugQuery?.value,
				explain: props.queryModel.explainQuery?.value,
			};
		});

		// Used for manual input of a JSON
		const queryJsonInput = ref("");

		const sendMoveError = ref("");
		function sendMove() {
			let move = {};

			move.endpointId = props.endpointId;
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
					queryStatus.value = "Preparing";
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

					queryStatus.value = "Submitted";
					const response = await userStore.authenticatedFetch(url, fetchOptions);
					if (!response.ok) {
						throw new NetworkError("POST has failed (" + response.statusText + " - " + response.status + ")", url, response);
					}

					queryStatus.value = "Downloading";

					const responseTabularView = await response.json();

					// This will be cancelled i nthe finally block: the rendering status is managed autonomously by the grid
					queryStatus.value = "Rendering";

					// The submitted move may have impacted the leaderboard
					store.$patch((state) => {
						store.queries["" + stringifiedQuery.hashCode()].result = responseTabularView;
						//state.contests[contestId].stale = true;
					});
					sendMoveError.value = "";

					// We need to couple the columns with the result
					// as the wizard may have been edited while receiving the result
					// We need both query and view to be assigned atomically, else some `watch` would trigger on partially updated object
					Object.assign(props.tabularView, { query: move.query, view: responseTabularView });
					// props.tabularView.value = {query: move.query, view: responseTabularView};

					// TODO Rely on a named route and params
					// router.push({ name: "board" });
				} catch (e) {
					console.error("Issue on Network:", e);
					sendMoveError.value = e.message;
				} finally {
					loading.value = false;
					queryStatus.value = "";
				}
			}

			return postFromUrl(`/cubes/query`);
		}

		// Watch for the query as a JSON: if it changes, we may trigger the query
		watch(
			() => queryJson.value,
			() => {
				if (autoQuery.value) {
					sendMove();
				}
			},
		);

		return {
			queryJson,
			autoQuery,

			sendMove,
			sendMoveError,

			loading,
			queryStatus,
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
			<!-- Move Submitter-->
			<span>
				<div>
				    <button type="button" @click="sendMove()" class="btn btn-outline-primary">Submit (queryStatus={{queryStatus}})
					</button>
				    <span v-if="sendMoveError" class="alert alert-warning" role="alert">{{sendMoveError}}</span>
				</div>

				<div class="form-check form-switch">
				  <input class="form-check-input" type="checkbox" role="switch" id="autoQuery" v-model="autoQuery">
				  <label class="form-check-label" for="autoQuery">autoQuery</label>
				</div>
			</span>

			<AdhocQueryRawModal :queryJson="queryJson" />
        </div>
    `,
};
