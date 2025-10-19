import { computed, ref, watch } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import AdhocQueryRawModal from "./adhoc-query-raw-modal.js";
import AdhocQueryReset from "./adhoc-query-reset.js";
import AdhocQueryFavorite from "./adhoc-query-favorite.js";
import AdhocQueryFavorites from "./adhoc-query-favorites.js";

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
		AdhocQueryRawModal,
		AdhocQueryReset,
		AdhocQueryFavorite,
		AdhocQueryFavorites,
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

		if (!props.queryModel.selectedColumns) {
			props.queryModel.selectedColumns = {};
		}
		if (!props.queryModel.selectedMeasures) {
			props.queryModel.selectedMeasures = {};
		}
		if (!props.queryModel.selectedOptions) {
			props.queryModel.selectedOptions = {};
		}

		// This computed property snapshots of the query
		const queryJson = computed(() => {
			//const columns = Object.keys(props.queryModel.selectedColumns).filter((column) => props.queryModel.selectedColumns[column] === true);
			const measures = Object.keys(props.queryModel.selectedMeasures).filter((measure) => props.queryModel.selectedMeasures[measure] === true);

			// Deep-Copy as the filter tree may be deep, and we must ensure it can not be edited while being executed
			const filter = JSON.parse(JSON.stringify(props.queryModel.filter || {}));

			// BEWARE This is a workaround to force `compute` to be reactive on all columns state
			// It is unclear why the reactivity on `props.queryModel.selectedColumnsOrdered` is not working
			const columns2 = Object.keys(props.queryModel.selectedColumns).filter((column) => props.queryModel.selectedColumns[column] === true);

			// https://stackoverflow.com/questions/597588/how-do-you-clone-an-array-of-objects-in-javascript
			// We do a copy as `queryJson` must not changed when playing with the wizard.
			// `.slice` as we want an immutable snapshot
			const orderedColumnsAsList = props.queryModel.selectedColumnsOrdered.slice(0);

			// Add a calculated member representing the grand totals
			const orderedColumnsWithStar = orderedColumnsAsList.map((c) => {
				if (props.queryModel.withStarColumns[c]) {
					return {
						type: ".ColumnWithCalculatedCoordinates",
						column: c,
						calculatedCoordinates: [
							{
								type: ".CalculatedCoordinate",
								coordinate: "*",
							},
						],
					};
				} else {
					return c;
				}
			});

			const orderedColumns = orderedColumnsWithStar;

			const options = Object.keys(props.queryModel.selectedOptions).filter((option) => props.queryModel.selectedOptions[option] === true);

			// Deep-Copy as the filter tree may be deep, and we must ensure it can not be edited while being executed
			const customMarkers = JSON.parse(JSON.stringify(props.queryModel.customMarkers || {}));

			return {
				groupBy: { columns: orderedColumns },
				measures: measures,
				filter: filter,
				options: options,
				customMarker: customMarkers,
			};
		});

		const latestSentQueryId = ref(-1);

		const onView = function (queryForApi, responseTabularView, stringifiedQuery, startDownloading) {
			props.tabularView.timing.downloading = new Date() - startDownloading;

			// This will be cancelled in the finally block: the rendering status is managed autonomously by the grid

			// The submitted move may have impacted the leaderboard
			store.$patch((state) => {
				store.queries["" + stringifiedQuery.hashCode()].result = responseTabularView;
				//state.contests[contestId].stale = true;
			});
			sendQueryError.value = "";

			// We need to couple the columns with the result
			// as the wizard may have been edited while receiving the result
			// We need both query and view to be assigned atomically, else some `watch` would trigger on partially updated object
			Object.assign(props.tabularView, { query: queryForApi.query, view: responseTabularView });
			// props.tabularView.value = {query: queryForApi.query, view: responseTabularView};

			// TODO Rely on a named route and params
			// router.push({ name: "board" });
		};

		const sendQueryError = ref("");
		function sendQuery() {
			let queryForApi = {};

			queryForApi.endpointId = props.endpointId;
			queryForApi.cube = props.cubeId;
			queryForApi.query = queryJson.value;

			// To be checked on after any `await`
			const latestSendQueryIdSnapshot = ++latestSentQueryId.value;
			console.info(`Submitting query for ${latestSendQueryIdSnapshot}`);

			async function postFromUrl(url) {
				try {
					loading.value = true;
					const stringifiedQuery = JSON.stringify(queryForApi);

					if (!store.queries["" + stringifiedQuery.hashCode()]) {
						store.queries["" + stringifiedQuery.hashCode()] = {};
					}
					store.queries["" + stringifiedQuery.hashCode()].query = queryForApi;

					const fetchOptions = {
						method: "POST",
						headers: { "Content-Type": "application/json" },
						body: stringifiedQuery,
					};

					if (!props.tabularView.loading) {
						props.tabularView.loading = {};
					}
					if (!props.tabularView.timing) {
						props.tabularView.timing = {};
					}

					const startSending = new Date();
					props.tabularView.loading.sending = true;

					const synchronous = false;

					if (synchronous) {
						const response = await userStore.authenticatedFetch(url, fetchOptions);
						if (latestSendQueryIdSnapshot !== latestSentQueryId.value) {
							console.warn(`Received response for ${latestSendQueryIdSnapshot} but latest query is ${latestSentQueryId.value}`);
							return;
						}

						props.tabularView.loading.sending = false;
						props.tabularView.timing.sending = new Date() - startSending;

						if (!response.ok) {
							throw new NetworkError("POST has failed (" + response.statusText + " - " + response.status + ")", url, response);
						}

						const startDownloading = new Date();
						props.tabularView.loading.downloading = true;

						const responseTabularView = await response.json();
						if (latestSendQueryIdSnapshot !== latestSentQueryId.value) {
							console.warn(`Received response.json for ${latestSendQueryIdSnapshot} but latest query is ${latestSentQueryId.value}`);
							return;
						}

						onView(queryForApi, responseTabularView, stringifiedQuery, startDownloading);
					} else {
						const response = await userStore.authenticatedFetch(url + "/asynchronous", fetchOptions);
						if (latestSendQueryIdSnapshot !== latestSentQueryId.value) {
							console.warn(`Received response for ${latestSendQueryIdSnapshot} but latest query is ${latestSentQueryId.value}`);
							return;
						}

						props.tabularView.loading.sending = false;
						props.tabularView.timing.sending = new Date() - startSending;

						if (!response.ok) {
							throw new NetworkError("POST has failed (" + response.statusText + " - " + response.status + ")", url, response);
						}

						const queryResultId = await response.json();
						console.info("Query has been registered as queryid=", queryResultId);

						const fetchStateOnlyOptions = {
							method: "GET",
							headers: { "Content-Type": "application/json" },
						};

						const startExecuting = new Date();
						props.tabularView.loading.executing = true;
						while (true) {
							// https://stackoverflow.com/questions/35038857/setting-query-string-using-fetch-get-request
							const responseStateOnly = await userStore.authenticatedFetch(
								url +
									"/result?" +
									new URLSearchParams({
										query_id: queryResultId,
										with_view: false,
									}).toString(),
								fetchStateOnlyOptions,
							);

							if (responseStateOnly.status !== 200) {
								const errorBody = await responseStateOnly.json();
								throw new Error("Issue fetch result: " + JSON.stringify(errorBody));
							}

							const responseStateOnlyJson = await responseStateOnly.json();
							console.debug("queryResult is", responseStateOnlyJson);

							if (responseStateOnlyJson.retryInMs) {
								const retryInMs = responseStateOnlyJson.retryInMs;
								console.log("Will retry in", retryInMs, "ms");

								// https://stackoverflow.com/questions/951021/what-is-the-javascript-version-of-sleep
								// sleep time expects milliseconds
								function sleep(time) {
									return new Promise((resolve) => setTimeout(resolve, time));
								}
								await sleep(retryInMs);
							} else {
								console.log("query is", responseStateOnlyJson.state);
								break;
							}
						}
						props.tabularView.loading.executing = false;
						props.tabularView.timing.executing = new Date() - startExecuting;

						const startDownloading = new Date();
						props.tabularView.loading.downloading = true;

						const fetchViewOptions = {
							method: "GET",
							headers: { "Content-Type": "application/json" },
						};
						// https://stackoverflow.com/questions/35038857/setting-query-string-using-fetch-get-request
						const responseView = await userStore.authenticatedFetch(
							url +
								"/result?" +
								new URLSearchParams({
									query_id: queryResultId,
									with_view: true,
								}).toString(),
							fetchViewOptions,
						);
						if (responseView.status !== 200) {
							const errorBody = await responseView.json();
							throw new Error("Issue fetch result: " + JSON.stringify(errorBody));
						}

						const responseTabularView = await responseView.json();
						if (latestSendQueryIdSnapshot !== latestSentQueryId.value) {
							console.warn(`Received response.json for ${latestSendQueryIdSnapshot} but latest query is ${latestSentQueryId.value}`);
							return;
						}

						if (responseTabularView.view) {
							onView(queryForApi, responseTabularView.view, stringifiedQuery, startDownloading);	
						} else {
							// Typically happens on a failure
							throw new Error("Query has state=" + responseTabularView.state);
						}
					}
				} catch (e) {
					console.error("Issue on Network:", e);
					sendQueryError.value = e.message;
				} finally {
					loading.value = false;
					props.tabularView.loading.sending = false;
					props.tabularView.loading.downloading = false;
				}
			}

			return postFromUrl(`/cubes/query`);
		}

		// Watch for the query as a JSON: if it changes, we may trigger the query
		watch(
			() => queryJson.value,
			() => {
				if (autoQuery.value) {
					sendQuery();
				}
			},
		);

		if (autoQuery.value) {
			console.log("Trigger queryExecution on component load");
			sendQuery();
		}

		return {
			queryJson,
			autoQuery,

			sendQuery,
			sendQueryError,

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
            <span>
                <div>
                    <button type="button" @click="sendQuery()" class="btn btn-outline-primary">Submit</button>
                    <span v-if="sendQueryError" class="alert alert-warning" role="alert">{{sendQueryError}}</span>
                </div>

                <div class="form-check form-switch">
                    <input class="form-check-input" type="checkbox" role="switch" id="autoQuery" v-model="autoQuery" />
                    <label class="form-check-label" for="autoQuery">autoQuery</label>
                </div>
            </span>

            <AdhocQueryRawModal :queryJson="queryJson" :queryModel="queryModel" />
            <AdhocQueryReset :queryModel="queryModel" />
            <AdhocQueryFavorite :queryModel="queryModel" />
            <AdhocQueryFavorites :queryModel="queryModel" />
        </div>
    `,
};
