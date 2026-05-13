// @ts-check
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
		// TODO Dead getter. The template never references `player`, the `players` key is not part of
		// the adhoc-store state, and the component's `props` declare `accountId` (not `playerId`). At
		// runtime this would throw `Cannot read properties of undefined (reading '...')`. Either
		// delete this getter or rewrite it against `store.accounts[this.accountId]`. The `any` casts
		// keep `// @ts-check` happy until that decision is made.
		...mapState(useAdhocStore, {
			player(store) {
				/** @type {any} */
				const self = this;
				return /** @type {any} */ (store).players[self.playerId] || { error: "not_loaded" };
			},
		}),
	},
	setup() {
		const userStore = useUserStore();

		userStore.initializeUser();

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
