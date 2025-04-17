import { computed, reactive, ref, watch, onMounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocEndpointHeader from "./adhoc-endpoint-header.js";
import AdhocCubeHeader from "./adhoc-cube-header.js";

import { useUserStore } from "./store-user.js";

import AdhocQueryWizard from "./adhoc-query-wizard.js";
import AdhocQueryExecutor from "./adhoc-query-executor.js";
import AdhocQueryGrid from "./adhoc-query-grid.js";

import { useRouter } from "vue-router";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocEndpointHeader,
		AdhocCubeHeader,
		AdhocQueryWizard,
		AdhocQueryExecutor,
		AdhocQueryGrid,
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

		const loading = ref(false);
		const queryModel = reactive({
			// `columnName->boolean`
			selectedColumns: {},
			// `measureName->boolean`
			selectedMeasures: {},
			// `orderedArray of columnNames`
			selectedColumnsOrdered: [],
		});
		const tabularView = reactive({});

		const router = useRouter();
		{
			const currentHashDecoded = router.currentRoute.value.hash;

			// Restore queryModel from URL hash
			if (currentHashDecoded && currentHashDecoded.startsWith("#")) {
				try {
					const currentHashObject = JSON.parse(currentHashDecoded.substring(1));
					const queryModelFromHash = currentHashObject.query;

					if (queryModelFromHash) {
						for (const [columnIndex, columnName] of Object.entries(queryModelFromHash.columns)) {
							queryModel.selectedColumns[columnName] = true;
							// Poor design: we have to compute manually selectedColumnsOrdered
							queryModel.selectedColumnsOrdered.push(columnName);
						}
						for (const [measureIndex, measureName] of Object.entries(queryModelFromHash.measures)) {
							queryModel.selectedMeasures[measureName] = true;
						}
						queryModel.filter = queryModelFromHash.filter;

						console.debug("queryModel after loading from hash: ", JSON.stringify(queryModel));
					}
				} catch (error) {
					console.warn("Issue parsing queryModel from hash", currentHashDecoded, error);
				}
			}

			// Save queryModel into URL hash
			watch(queryModel, async (newQueryModel) => {
				const currentHashDecoded = router.currentRoute.value.hash;

				var currentHashObject;
				if (currentHashDecoded && currentHashDecoded.startsWith("#")) {
					currentHashObject = JSON.parse(currentHashDecoded.substring(1));
				} else {
					currentHashObject = {};
				}
				currentHashObject.query = {};
				currentHashObject.query.columns = Object.values(newQueryModel.selectedColumnsOrdered);
				currentHashObject.query.measures = Object.keys(newQueryModel.selectedMeasures).filter((measure) => newQueryModel.selectedMeasures[measure] === true);
				currentHashObject.query.filter = newQueryModel.filter;

				console.debug("Saving queryModel to hash", JSON.stringify(newQueryModel));

				// https://stackoverflow.com/questions/51337255/silently-update-url-without-triggering-route-in-vue-router
				const newUrl = router.currentRoute.value.path + "#" + encodeURIComponent(JSON.stringify(currentHashObject));
				history.pushState({}, null, newUrl);
			});
		}

		// SlickGrid requires a cssSelector
		const domId = ref("slickgrid_" + Math.floor(Math.random() * 1024));

		return {
			loading,
			queryModel,
			tabularView,
			domId,
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
            <AdhocCubeHeader :endpointId="endpointId" :cubeId="cubeId" />

            <div class="row">
                <div class="col-3">
                    <div class="row">
                        <AdhocQueryWizard :endpointId="endpointId" :cubeId="cubeId" :queryModel="queryModel" :loading="loading" />
                    </div>

                    <div class="row">
                        <AdhocQueryExecutor :endpointId="endpointId" :cubeId="cubeId" :queryModel="queryModel" :tabularView="tabularView" :loading="loading" />
                    </div>
                </div>
                <div class="col-9">
                    <AdhocQueryGrid :tabularView="tabularView" :loading="loading" :queryModel="queryModel" :domId="domId" />
                </div>
            </div>
        </div>
    `,
};
