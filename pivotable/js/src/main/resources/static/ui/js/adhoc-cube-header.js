import { ref, onMounted, onUnmounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import AdhocCubeRef from "./adhoc-cube-ref.js";
import AdhocAccountRef from "./adhoc-account-ref.js";
import AdhocEndpointRef from "./adhoc-endpoint-ref.js";

export default {
	components: {
		AdhocCubeRef,
		AdhocAccountRef,
		AdhocEndpointRef,
	},
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
		...mapState(useAdhocStore, ["nbSchemaFetching", "nbCubeFetching", "isLoggedIn", "account"]),
		...mapState(useAdhocStore, {
			endpoint(store) {
				return store.endpoints[this.endpointId] || { error: "not_loaded" };
			},
			cube(store) {
				return store.schemas[this.endpointId]?.cubes[this.cubeId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadCubeSchemaIfMissing(props.cubeId, props.endpointId);

		return {};
	},
	template: /* HTML */ `
        <div v-if="(!endpoint || !cube) && (nbSchemaFetching > 0 || nbCubeFetching > 0)">
            <div class="spinner-border" role="status">
                <span class="visually-hidden">Loading cubeId={{cubeId}}</span>
            </div>
        </div>
        <div v-else-if="endpoint.error || cube.error">{{endpoint.error || cube.error}}</div>
        <span v-else>
            <h2>
                <AdhocCubeRef :cubeId="cubeId" :endpointId="endpointId" />
                <AdhocEndpointRef :endpointId="endpointId" />
            </h2>
        </span>
    `,
};
