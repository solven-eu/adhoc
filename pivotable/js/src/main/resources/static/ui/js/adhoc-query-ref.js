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
		entrypointId: {
			type: String,
			required: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, {
			cube(store) {
				return store.schemas[this.entrypointId]?.cubes[this.cubeId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadCubeSchemaIfMissing(props.cubeId, props.entrypointId);

		return {};
	},
	template: /* HTML */ `
        <RouterLink :to="{path:'/html/entrypoints/' + entrypointId + '/cubes/' + cubeId + '/query'}">
            <i class="bi bi-bar-chart"></i> {{cubeId}}
        </RouterLink>
    `,
};
