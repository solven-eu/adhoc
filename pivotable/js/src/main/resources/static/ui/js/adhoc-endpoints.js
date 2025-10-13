import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import LoginRef from "./login-ref.js";

import AdhocEndpoint from "./adhoc-endpoint.js";

export default {
	components: {
		LoginRef,
		AdhocEndpoint,
	},
	computed: {
		...mapState(useAdhocStore, ["isLoggedIn", "nbSchemaFetching"]),
		...mapState(useAdhocStore, {
			endpoints(store) {
				return Object.values(store.endpoints);
			},
		}),
	},
	setup() {
		const store = useAdhocStore();

		store.loadEndpoints();

		return {};
	},
	template: /* HTML */ `
        <div v-if="!isLoggedIn"><LoginRef /></div>
        <div v-if="Object.keys(endpoints).length == 0">
            <div v-if="nbSchemaFetching > 0">Loading endpoints</div>
            <div v-else>Issue loading endpoints (or no endpoints at all)</div>
        </div>
        <div v-else class="container">
            <div class="row border" v-for="endpoint in endpoints">
                <AdhocEndpoint :endpointId="endpoint.id" :showSchema="false" />
            </div>
        </div>
    `,
};
