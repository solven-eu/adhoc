import { computed, reactive, ref, watch, onMounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocEndpointHeader from "./adhoc-endpoint-header.js";
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
		}),
	},
	setup(props) {
		const store = useAdhocStore();
		const userStore = useUserStore();

		store.loadCubeSchemaIfMissing(props.cubeId, props.endpointId);

		const coordinates = reactive({ array: [] });
		const loading = ref(false);

		function loadColumnCoordinates() {
			loading.value = true;
			store
				.loadColumnCoordinates(props.cubeId, props.endpointId, props.column)
				.then((columnMeta) => {
					coordinates.array = columnMeta;
				})
				.finally(() => {
					loading.value = false;
				});
		}
		
		watch(() => props.queryModel.selectedColumns[props.column], (newX) => {
			if (!props.queryModel.selectedColumns2) {
				props.queryModel.selectedColumns2 = [];
			}

			const array = props.queryModel.selectedColumns2;
			const index = array.indexOf(props.column);
			if (newX) {
				if (index < 0) {
					props.queryModel.selectedColumns2.push(props.column);	
				}
			} else {
				// https://stackoverflow.com/questions/5767325/how-can-i-remove-a-specific-item-from-an-array-in-javascript
				// only splice array when item is found
				if (index >= 0) { 
					// 2nd parameter means remove one item only
				  array.splice(index, 1); 
				}
			}
			
		  console.log(`${props.column} is ${newX}`)
		});

		return {
			loadColumnCoordinates,
			loading,
			coordinates,
		};
	},
	template: /* HTML */ `
	<div class="form-check form-switch">
	  <input class="form-check-input" type="checkbox" role="switch" :id="'column_' + column" v-model="queryModel.selectedColumns[column]" />
	  <label class="form-check-label" :for="'column_' + column">{{column}}: {{type}}</label>
	</div>
	
	  	<button  type="button" @click="loadColumnCoordinates()" class="badge bg-primary rounded-pill">

		? - {{coordinates.array.length}}
		<span v-if="loading">

		<div class="spinner-grow" role="status">
		  <span class="visually-hidden">Loading...</span>
		</div>
		
		</span>
		</button>
    `,
};
