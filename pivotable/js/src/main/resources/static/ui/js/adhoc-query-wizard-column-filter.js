import { computed, reactive, ref, watch, onMounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import { useUserStore } from "./store-user.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {},
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

		column: {
			type: String,
			required: true,
		},
		type: {
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
			columnMeta(store) {
				const columnId = `${this.endpointId}-${this.cubeId}-${this.column}`;
				return store.columns[columnId] || { error: "not_loaded" };
			},
		}),
	},
	methods: {
		// https://stackoverflow.com/questions/42632711/how-to-call-function-on-child-component-on-parent-events
		//    saveFilter: function(value) {
		//        this.value = value;
		//    }
	},
	setup(props) {
		const store = useAdhocStore();
		const userStore = useUserStore();

		store.loadCubeSchemaIfMissing(props.cubeId, props.endpointId);

		// Do not load here, else all columns would be loaded when loading the wizard even if columns are not visible
		// store.loadColumnCoordinatesIfMissing(props.cubeId, props.endpointId, props.column);

		const filterTypes = ["equals", "like", "json"];
		const filterType = ref("no_filter");

		const equalsValue = ref("");

		const rawFilterAsJson = ref("rawFilterAsJson");

		const pendingChanges = ref(false);

		const columnMeta = computed(() => {
			const columnId = `${props.endpointId}-${props.cubeId}-${props.column}`;
			return store.columns[columnId] || { error: "not_loaded" };
		});

		watch(filterType, () => {
			pendingChanges.value = true;

			// The User selected `equals` filter: ensure we have a subset of coordinate to help him making his filter
			if (filterType.value === "equals" && columnMeta.value.error === "not_loaded") {
				store.loadColumnCoordinatesIfMissing(props.cubeId, props.endpointId, props.column);
			}
		});
		watch(equalsValue, () => {
			pendingChanges.value = true;
		});
		watch(rawFilterAsJson, () => {
			pendingChanges.value = true;
		});

		// https://stackoverflow.com/questions/42632711/how-to-call-function-on-child-component-on-parent-events
		function saveFilter(value) {
			//			{
			//			  "type" : "and",
			//			  "filters" : [ {
			//			    "type" : "column",
			//			    "column" : "a",
			//			    "valueMatcher" : "a1",
			//			    "nullIfAbsent" : true
			//			  }, {
			//			    "type" : "column",
			//			    "column" : "b",
			//			    "valueMatcher" : "b2",
			//			    "nullIfAbsent" : true
			//			  } ]
			//			}
			if (!props.queryModel.filter || !props.queryModel.filter.type) {
				props.queryModel.filter = {};
				props.queryModel.filter.type = "and";
				props.queryModel.filter.filters = [];
			} else if (props.queryModel.filter.type !== "and") {
				throw new Error("We support only 'and'");
			}

			// https://stackoverflow.com/questions/15995963/remove-array-element-on-condition
			props.queryModel.filter.filters = props.queryModel.filter.filters.filter((item) => item.column !== props.column);

			if (filterType.value == "no_filter") {
				console.log("No filter on", props.column);
			} else if (filterType.value == "equals") {
				console.log("filter", props.column, "equals", equalsValue.value);
				const columnFilter = { type: "column", column: props.column, valueMatcher: equalsValue.value };
				props.queryModel.filter.filters.push(columnFilter);
				rawFilterAsJson.value = JSON.stringify(columnFilter);
			} else {
				props.queryModel.filter.filters.push(JSON.parse(rawFilterAsJson));
			}

			pendingChanges.value = false;
		}

		return {
			filterTypes,
			filterType,

			equalsValue,

			rawFilterAsJson,

			pendingChanges,
			saveFilter,
		};
	},
	template: /* HTML */ `
        <div>
            column={{column}} filterType={{filterType}}
            <select class="form-select" aria-label="Filter type" v-model="filterType">
                <option value="no_filter">No filtering</option>
                <option v-for="value in filterTypes" :value="value">{{value}}</option>
            </select>

            <div v-if="filterType == 'no_filter'"></div>
            <div v-else-if="filterType == 'equals'">
                <input v-model="equalsValue" placeholder="single value" />

                <select class="form-select" aria-label="Filter type" v-model="equalsValue">
                    <option disabled value="no_value">Please select a value</option>
                    <option v-for="coordinate in columnMeta.coordinates" :value="coordinate">{{coordinate}}</option>
                </select>
            </div>
            <div v-else>
                <textarea v-model="rawFilterAsJson" placeholder="raw filter as json"></textarea>
            </div>

            pending={{pendingChanges}}
        </div>
    `,
};
