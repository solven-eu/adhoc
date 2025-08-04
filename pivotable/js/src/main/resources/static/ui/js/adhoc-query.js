import { reactive, ref, watch, provide } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import queryHelper from "./adhoc-query-helper.js";

import AdhocEndpointHeader from "./adhoc-endpoint-header.js";
import AdhocCubeHeader from "./adhoc-cube-header.js";

import { useUserStore } from "./store-user.js";

import AdhocQueryWizard from "./adhoc-query-wizard.js";
import AdhocQueryExecutor from "./adhoc-query-executor.js";
import AdhocQueryGrid from "./adhoc-query-grid.js";

import { useRouter } from "vue-router";

import AdhocMeasuresDag from "./adhoc-measures-dag.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocEndpointHeader,
		AdhocCubeHeader,
		AdhocQueryWizard,
		AdhocQueryExecutor,
		AdhocQueryGrid,
		AdhocMeasuresDag,
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

		cube: {
			type: Object,
			required: true,
		},
	},
	computed: {
		...mapState(useUserStore, ["needsToLogin"]),
		...mapState(useAdhocStore, ["nbSchemaFetching"]),
		...mapState(useAdhocStore, {
			endpoint(store) {
				return store.endpoints[this.endpointId] || { error: "not_loaded" };
			},
			schema(store) {
				return store.schemas[this.endpointId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadCubeSchemaIfMissing(props.cubeId, props.endpointId);

		const loading = ref(false);
		const queryModel = reactive(queryHelper.makeQueryModel());

		// Watch for changes on `selectedColumns` to update `selectedColumnsOrdered` accordingly
		watch(
			() => queryModel.selectedColumns,
			(newX) => {
				queryModel.onColumnToggled();
			},
			{ deep: true },
		);

		const measuresDagModel = reactive({
			main: "",
			highlight: [],
		});

		// https://vuejs.org/guide/components/provide-inject.html
		provide("queryModel", queryModel);
		provide("cube", props.cube);
		provide("measuresDagModel", measuresDagModel);

		const tabularView = reactive({});

		const router = useRouter();
		{
			const currentHashDecoded = router.currentRoute.value.hash;

			queryHelper.hashToQueryModel(currentHashDecoded, queryModel);

			// Save queryModel into URL hash
			watch(queryModel, async (newQueryModel) => {
				const currentHashDecoded = router.currentRoute.value.hash;

				const newHash = queryHelper.queryModelToHash(currentHashDecoded, newQueryModel);

				// https://stackoverflow.com/questions/51337255/silently-update-url-without-triggering-route-in-vue-router
				const newUrl = router.currentRoute.value.path + newHash;

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
			measuresDagModel,
		};
	},
	template: /* HTML */ `
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
                <AdhocQueryGrid :tabularView="tabularView" :loading="loading" :queryModel="queryModel" :domId="domId" :cube="cube" />
            </div>

            <AdhocMeasuresDag :measuresDagModel="measuresDagModel" />
        </div>
    `,
};
