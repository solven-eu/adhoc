// @ts-check
import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import AdhocCubeChip from "./adhoc-cube-chip.js";
import AdhocAccountChip from "./adhoc-account-chip.js";
import AdhocEndpointChip from "./adhoc-endpoint-chip.js";

export default {
	components: {
		AdhocCubeChip,
		AdhocAccountChip,
		AdhocEndpointChip,
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
		// Drop the dead-but-referenced `nbCubeFetching` (no store ever defined it; the template condition
		// below evaluated it as `undefined > 0` = false, so the only effective gate was `nbSchemaFetching > 0`).
		...mapState(useAdhocStore, ["nbSchemaFetching", "isLoggedIn", "account"]),
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

		return {};
	},
	template: /* HTML */ `
		<div v-if="(!endpoint || !cube) && nbSchemaFetching > 0">
			<div class="spinner-border" role="status">
				<span class="visually-hidden">Loading cubeId={{cubeId}}</span>
			</div>
		</div>
		<div v-else-if="endpoint.error || cube.error">{{endpoint.error || cube.error}}</div>
		<span v-else>
			<h2>
				<AdhocCubeChip :cubeId="cubeId" :endpointId="endpointId" />
				<AdhocEndpointChip :endpointId="endpointId" />
			</h2>
		</span>
	`,
};
