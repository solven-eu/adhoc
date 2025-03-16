import {} from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

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
        <RouterLink :to="{path:'/html/endpoints/' + endpointId + '/cubes/' + cubeId + '/query'}"> <i class="bi bi-grid-3x3"></i> Query {{cubeId}} </RouterLink>
    `,
};
