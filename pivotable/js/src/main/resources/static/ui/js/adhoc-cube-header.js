import { ref, onMounted, onUnmounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocCubeRef from "./adhoc-cube-ref.js";
import AdhocAccountRef from "./adhoc-account-ref.js";
import AdhocEntrypointRef from "./adhoc-entrypoint-ref.js";

export default {
	components: {
		AdhocCubeRef,
		AdhocAccountRef,
		AdhocEntrypointRef,
	},
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
		...mapState(useAdhocStore, ["nbSchemaFetching", "nbCubeFetching", "isLoggedIn", "account"]),
		...mapState(useAdhocStore, {
			entrypoint(store) {
				return store.entrypoints[this.entrypointId] || { error: "not_loaded" };
			},
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
        <div v-if="(!entrypoint || !cube) && (nbSchemaFetching > 0 || nbCubeFetching > 0)">
            <div class="spinner-border" role="status">
                <span class="visually-hidden">Loading cubeId={{cubeId}}</span>
            </div>
        </div>
        <div v-else-if="entrypoint.error || cube.error">{{entrypoint.error || cube.error}}</div>
        <span v-else>
            <h2>
                <AdhocCubeRef :cubeId="cubeId" :entrypointId="entrypointId" />
				<AdhocEntrypointRef :entrypointId="entrypointId" />
            </h2>

            {{cubeId}}
        </span>
    `,
};
