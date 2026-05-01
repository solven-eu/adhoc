import { mapState } from "pinia";
import { useUserStore } from "./store-user.js";

export default {
	props: {
		modal: {
			type: Boolean,
			default: false,
		},
	},
	computed: {
		...mapState(useUserStore, ["needsToCheckLogin", "nbLoginLoading", "isLoggedIn", "isLoggedOut"]),
	},
	setup() {
		return {};
	},
	template: /* HTML */ `
		<span v-if="isLoggedIn" :hidden="!modal">You are logged in</span>
		<span v-else-if="isLoggedOut">
			<RouterLink :to="{path:'/html/login'}"><i class="bi bi-person"></i> You need to login</RouterLink>
		</span>
		<span v-else-if="needsToCheckLogin">
			<span v-if="nbLoginLoading > 0">
				<span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>
				Loading account…
			</span>
			<span v-else>Unclear login status but not loading. Should not happen</span>
		</span>
		<span v-else> This should not happen (login-chip.js) </span>
	`,
};
