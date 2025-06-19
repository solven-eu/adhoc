import { reactive, ref, watch, provide } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import AdhocEndpointHeader from "./adhoc-endpoint-header.js";
import AdhocCubeHeader from "./adhoc-cube-header.js";

import { useUserStore } from "./store-user.js";

import AdhocQueryWizard from "./adhoc-query-wizard.js";
import AdhocQueryExecutor from "./adhoc-query-executor.js";
import AdhocQueryGrid from "./adhoc-query-grid.js";

import { useRouter } from "vue-router";

import AdhocMeasuresDag from "./adhoc-measures-dag.js";

import wizardHelper from "./adhoc-query-wizard-helper.js";

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
		const queryModel = reactive({
			// `columnName->boolean`
			selectedColumns: {},
			// `measureName->boolean`
			selectedMeasures: {},
			// `orderedArray of columnNames`
			selectedColumnsOrdered: [],
			customMarkers: {},
			// `optionName->boolean`
			options: {},

			onColumnToggled: function (column) {
				const array = queryModel.selectedColumnsOrdered;

				if (!column) {
					// We lack knowledge about which columns has been toggled
					for (const column of Object.keys(queryModel.selectedColumns)) {
						const index = array.indexOf(column);

						let isChanged = false;

						// May be missing on first toggle
						const toggledIn = !!queryModel.selectedColumns[column];
						if (toggledIn) {
							if (index < 0) {
								// Append the column
								array.push(column);
								isChanged = true;
							}
						} else {
							// https://stackoverflow.com/questions/5767325/how-can-i-remove-a-specific-item-from-an-array-in-javascript
							// only splice array when item is found
							if (index >= 0) {
								// 2nd parameter means remove one item only
								array.splice(index, 1);
								isChanged = true;
							}
						}
						if (isChanged) {
							console.log(`groupBy: ${column} is now ${toggledIn}`);
						} else {
							console.debug(`groupBy: ${column} is kept ${toggledIn}`);
						}
					}
				} else {
					const index = array.indexOf(column);

					// May be missing on first toggle
					const toggledIn = !!queryModel.selectedColumns[column];
					if (toggledIn) {
						if (index < 0) {
							// Append the column
							array.push(column);
						} else {
							console.warn("Adding a column already here?", column);
						}
					} else {
						// https://stackoverflow.com/questions/5767325/how-can-i-remove-a-specific-item-from-an-array-in-javascript
						// only splice array when item is found
						if (index >= 0) {
							// 2nd parameter means remove one item only
							array.splice(index, 1);
						} else {
							console.warn("Removing a column already absent?", column);
						}
					}
					console.log(`groupBy: ${column} is now ${toggledIn}`);
				}
			},
		});

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

		const queried = function (arrayOrObject) {
			return wizardHelper.queried(arrayOrObject);
		};

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
							queryModel.onColumnToggled(columnName);
						}
						for (const [measureIndex, measureName] of Object.entries(queryModelFromHash.measures)) {
							queryModel.selectedMeasures[measureName] = true;
						}
						queryModel.filter = queryModelFromHash.filter || {};
						queryModel.customMarkers = queryModelFromHash.customMarkers || {};
						queryModel.options = queryModelFromHash.options || {};

						console.debug("queryModel after loading from hash: ", JSON.stringify(queryModel));
					}
				} catch (error) {
					// log but not re-throw as we do not want the hash to prevent the application from loading
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
				currentHashObject.query.measures = queried(newQueryModel.selectedMeasures);
				currentHashObject.query.filter = newQueryModel.filter || {};
				currentHashObject.query.customMarkers = newQueryModel.customMarkers || {};
				currentHashObject.query.options = queried(newQueryModel.options) || {};

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
