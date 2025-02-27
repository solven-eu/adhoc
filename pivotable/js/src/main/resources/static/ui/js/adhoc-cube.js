import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocEntrypointHeader from "./adhoc-entrypoint-header.js";
import AdhocCubeHeader from "./adhoc-cube-header.js";

import AdhocQueryRef from "./adhoc-query-ref.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocEntrypointHeader,
		AdhocCubeHeader,
		AdhocQueryRef,
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
		showEntrypoint: {
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

            <AdhocEntrypointHeader :entrypointId="entrypointId" :withDescription="false" v-if="showEntrypoint" />
			
			<AdhocQueryRef :cubeId="cubeId" :entrypointId="entrypointId" :withDescription="false" v-if="showEntrypoint" />
        </div>
    `,
};
