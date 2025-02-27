import { computed, reactive, ref, watch, onMounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocEntrypointHeader from "./adhoc-entrypoint-header.js";
import AdhocCubeHeader from "./adhoc-cube-header.js";

import AdhocMeasure from "./adhoc-measure.js";

import { useUserStore } from "./store-user.js";

import AdhocQueryWizard from "./adhoc-query-wizard.js";
import AdhocQueryExecutor from "./adhoc-query-executor.js";
import AdhocQueryView from "./adhoc-query-grid.js";

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
		AdhocQueryWizard,
		AdhocQueryExecutor,
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

		const loading = ref(false);
		const queryModel = reactive({selectedColumns: {}, selectedMeasures: {}});
		const tabularView = reactive({});

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
			
			<div class="row">
			  <div class="col">
			<div class="row">
			<AdhocQueryWizard :entrypointId="entrypointId" :cubeId="cubeId" :queryModel="queryModel" :loading="loading" />
			  </div>

			  <div class="row">
			  <AdhocQueryExecutor :entrypointId="entrypointId" :cubeId="cubeId" :queryModel="queryModel" :tabularView="tabularView" :loading="loading" />
			    </div>
			  </div>
			  <div class="col-9">
			  <AdhocQueryView :tabularView="tabularView" :loading="loading" :domId="domId" />
			  </div>
			</div>

        </div>
    `,
};
