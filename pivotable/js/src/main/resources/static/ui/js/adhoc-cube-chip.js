// @ts-check
import {} from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

export default {
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
	},
	computed: {
		...mapState(useAdhocStore, {
			cube(store) {
				// `this` is the Vue Options-API component instance (props/data); pinia's `mapState` types
				// don't know about that context, so cast it to read `endpointId` / `cubeId` props.
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
	template: /* HTML */ ` <RouterLink :to="{path:'/html/endpoints/' + endpointId + '/cubes/' + cubeId}"> <i class="bi bi-box"></i> {{cubeId}} </RouterLink> `,
};
