import { computed, reactive, ref, watch, onMounted, provide } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import AdhocEndpointHeader from "./adhoc-endpoint-header.js";
import AdhocCubeHeader from "./adhoc-cube-header.js";

import { useUserStore } from "./store-user.js";

import AdhocQuery from "./adhoc-query.js";

import { useRouter } from "vue-router";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocQuery,
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
	},
	computed: {
		...mapState(useUserStore, ["needsToLogin"]),
		...mapState(useAdhocStore, ["nbSchemaFetching"]),
		...mapState(useAdhocStore, {
			endpoint(store) {
				return store.endpoints[this.endpointId] || { error: "not_loaded" };
			},
			schema(store) {
				return store.schemas[this.endpointId] || { error: "not_loaded" };
			},
			cube(store) {
				const endpoint = store.schemas[this.endpointId];

				if (!endpoint) {
					return { error: "endpoint_not_loaded" };
				} else if (endpoint.error) {
					return { error: "endpoint_error=" + endpoint.error };
				}

				return endpoint.cubes[this.cubeId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();
		const userStore = useUserStore();

		store.loadCubeSchemaIfMissing(props.cubeId, props.endpointId);

		watch(
			() => userStore.needsToLogin,
			(newLoginState) => {
				console.log("needsToLogin", newLoginState);

				store.loadCubeSchemaIfMissing(props.cubeId, props.endpointId);
			},
		);

		return { store };
	},
	template: /* HTML */ `
        <div v-if="needsToLogin">Needs to login</div>
        <div v-else-if="(!endpoint || !cube)">
            <div v-if="(nbSchemaFetching > 0 || nbContestFetching > 0)">
                <div class="spinner-border" role="status">
                    <span class="visually-hidden">Loading cubeId={{cubeId}}</span>
                </div>
            </div>
            <div v-else>
                <span>Issue loading cubeId={{cubeId}}</span>
            </div>
        </div>
        <div v-else-if="endpoint.error">Endpoint error: {{endpoint.error}}</div>
        <div v-else-if="cube.error">Cube error: {{cube.error}}</div>
        <div v-else>
            <AdhocQuery :endpointId="endpointId" :cubeId="cubeId" :cube="cube" />
        </div>
    `,
};
