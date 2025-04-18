import {} from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";
import { useUserStore } from "./store-user.js";

import Flag from "./flag.js";

export default {
	components: {
		Flag,
	},
	props: {
		accountId: {
			type: String,
			required: true,
		},
	},
	computed: {
		...mapState(useUserStore, ["account"]),
		...mapState(useAdhocStore, {
			player(store) {
				return store.players[this.playerId] || { error: "not_loaded" };
			},
		}),
	},
	setup() {
		const userStore = useUserStore();

		userStore.loadUser();

		return {};
	},
	template: /* HTML */ `
        <span v-if="account.accountId === accountId">
            <RouterLink :to="{path:'/html/me'}">
                <i class="bi bi-person"></i>accountId: {{ accountId }}<span> (You)</span>

                <Flag :country="account.details.countryCode" v-if="account.details.countryCode" />
            </RouterLink>
        </span>
        <span v-else>
            <RouterLink :to="{path:'/html/accounts/' + accountId}">
                <i class="bi bi-person"></i>accountId: {{ accountId }}

                <Flag :country="account.details.countryCode" v-if="account.details.countryCode" />
            </RouterLink>
        </span>
    `,
};
