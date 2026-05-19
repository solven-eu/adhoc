// @ts-check
import { provide } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

// Cube + endpoint identification is now shown via the navbar breadcrumb
// (Endpoints / endpoint / cube), so AdhocCubeHeader is no longer mounted on this route.
import AdhocQueryChip from "./adhoc-query-chip.js";

import AdhocLoading from "./adhoc-loading.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocQueryChip,
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
				const self = /** @type {any} */ (this);
				return store.endpoints[self.endpointId] || { error: "not_loaded" };
			},
			cube(store) {
				const self = /** @type {any} */ (this);
				return store.schemas[self.endpointId]?.cubes[self.cubeId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadCubeSchemaIfMissing(props.cubeId, props.endpointId);

		// https://vuejs.org/guide/components/provide-inject.html
		provide("ids", { cubeId: props.cubeId, endpointId: props.endpointId });

		return {};
	},
	template: /* HTML */ `
		<div v-if="!endpoint || endpoint.error || !cube || cube.error">
			<AdhocLoading :id="endpointId" type="endpoint" :loading="nbSchemaFetching > 0" :error="endpoint.error" />
			<AdhocLoading :id="cubeId" type="cube" :loading="nbSchemaFetching > 0" :error="cube.error" />
		</div>
		<div v-else>
			<ul>
				<li><AdhocQueryChip :cubeId="cubeId" :endpointId="endpointId" :withDescription="false" v-if="showEndpoint" /></li>
			</ul>
		</div>
	`,
};
