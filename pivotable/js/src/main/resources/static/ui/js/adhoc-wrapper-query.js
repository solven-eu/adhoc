import { watch } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import { useUserStore } from "./store-user.js";

import AdhocQuery from "./adhoc-query.js";
import AdhocLoading from "./adhoc-loading.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocQuery,
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
	},
	computed: {
		...mapState(useUserStore, ["nbLoginLoading", "needsToLogin"]),
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

		// store.loadCubeSchemaIfMissing(props.cubeId, props.endpointId);

		// Needs to be logged-in to do queries
		userStore.initializeUser().then((user) => {
			if (user.error) {
				console.info("Do not load schema as user.error", user.error);

				// Ensure the app knows we want to login
				userStore.needsToLogin = true;
			} else {
				store.loadCubeSchemaIfMissing(props.cubeId, props.endpointId);
			}
		});

		watch(
			() => userStore.isLoggedIn,
			(newLoginState) => {
				if (newLoginState) {
					store.loadCubeSchemaIfMissing(props.cubeId, props.endpointId);
				} else {
					userStore.needsToLogin = true;
				}
			},
			{ deep: true },
		);

		return { store };
	},
	template: /* HTML */ `
        <div v-if="needsToLogin">Needs to login</div>
        <div v-else-if="!endpoint || endpoint.error || !cube || cube.error">
			<AdhocLoading :id="cubeId" :loading="nbSchemaFetching > 0" :error="cube.error" />
			<AdhocLoading :id="endpointId" :loading="nbSchemaFetching > 0" :error="endpoint.error" />
			<AdhocLoading id="login" :loading="nbLoginLoading > 0" :error="needsToLogin ? 'needsToLogin' : null" />
        </div>
        <div v-else>
            <AdhocQuery :endpointId="endpointId" :cubeId="cubeId" :cube="cube" />
        </div>
    `,
};
