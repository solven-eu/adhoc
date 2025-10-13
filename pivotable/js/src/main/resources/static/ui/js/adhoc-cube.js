import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import AdhocEndpointHeader from "./adhoc-endpoint-header.js";
import AdhocCubeHeader from "./adhoc-cube-header.js";

import AdhocQueryRef from "./adhoc-query-ref.js";

import AdhocLoading from "./adhoc-loading.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocEndpointHeader,
		AdhocCubeHeader,
		AdhocQueryRef,
		AdhocLoading,
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
		showEndpoint: {
			type: Boolean,
			default: true,
		},
		showLeaderboard: {
			type: Boolean,
			default: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbSchemaFetching"]),
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
        <div v-if="!endpoint || endpoint.error || !cube || cube.error">
            <AdhocLoading :id="endpointId" type="endpoint" :loading="nbSchemaFetching > 0" :error="endpoint.error" />
            <AdhocLoading :id="cubeId" type="cube" :loading="nbSchemaFetching > 0" :error="cube.error" />
        </div>
        <div v-else>
            <AdhocCubeHeader :endpointId="endpointId" :cubeId="cubeId" />

            <ul>
                <li><AdhocQueryRef :cubeId="cubeId" :endpointId="endpointId" :withDescription="false" v-if="showEndpoint" /></li>
            </ul>
        </div>
    `,
};
