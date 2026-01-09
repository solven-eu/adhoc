import { watch } from "vue";
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
		...mapState(useAdhocStore, ["needsToCheckLogin", "isLoggedIn", "nbSchemaFetching"]),
		...mapState(useAdhocStore, {
			endpoints(store) {
				return Object.values(store.endpoints);
			},
		}),
	},
	setup() {
		const store = useAdhocStore();

		watch(() => store.isLoggedIn, (isLoggedIn) => {
			if (isLoggedIn) {
				store.loadEndpoints();
			} else {
				
			}
		});

		return {};
	},
	template: /* HTML */ `
		<div v-if="needsToCheckLogin">
			Loading the login status...
		</div>
        <div v-else-if="!isLoggedIn">
			Needs to be logged-in to fetch endpoints.
			<br/>
			<LoginRef />
		</div>
        <div v-else-if="Object.keys(endpoints).length == 0">
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
