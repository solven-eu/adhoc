import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import LoginRef from "./login-ref.js";

import AdhocGame from "./adhoc-server.js";

export default {
	components: {
		LoginRef,
		AdhocGame,
	},
	computed: {
		...mapState(useAdhocStore, ["isLoggedIn", "nbServerFetching"]),
		...mapState(useAdhocStore, {
			servers(store) {
				return Object.values(store.servers);
			},
		}),
	},
	setup() {
		const store = useAdhocStore();

		store.loadGames();

		return {};
	},
	template: /* HTML */ `
        <div v-if="!isLoggedIn"><LoginRef /></div>
        <div v-if="Object.keys(servers) == 0">
            <div v-if="nbServerFetching > 0">Loading servers</div>
            <div v-else>Issue loading servers (or no servers at all)</div>
        </div>
        <div v-else class="container">
            <div class="row border" v-for="server in servers">
                <AdhocGame :serverId="server.serverId" :showContests="false" v-if="!server.error" />
            </div>
        </div>
    `,
};
