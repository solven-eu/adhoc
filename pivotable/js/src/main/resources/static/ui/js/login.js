import { mapState } from "pinia";
import { useUserStore } from "./store-user.js";

import LoginOptions from "./login-providers.js";
import Logout from "./login-logout.js";

import Whatnow from "./whatnow.js";

export default {
	components: {
		LoginOptions,
		Logout,
		Whatnow,
	},
	props: {
		success: {
			type: Boolean,
			required: true,
		},
		logout: {
			type: Boolean,
			required: true,
		},
	},
	computed: {
		...mapState(useUserStore, ["nbLoginLoading", "account", "isLoggedIn", "isLoggedOut"]),
		...mapState(useUserStore, {
			user(store) {
				return store.account;
			},
		}),
	},
	setup(props) {
		const userStore = useUserStore();

		const hintLoginSuccess = props.success;
		const hintLoggedOut = props.logout;

		// load tokens for current user
		userStore.initializeUserTokens();

		return { userStore, hintLoginSuccess, hintLoggedOut };
	},
	template: /* HTML */ `isLoggedIn={{isLoggedIn}} userStore={{userStore.needsToLogin}} user={{user.details.username}}
		<div v-if="isLoggedIn">
			Welcome {{user.details.name}}. <Logout />

			<Whatnow />

			<span v-if="hintLoginSuccess"> Login Success </span>
			<span v-if="hintLoggedOut"> Logout Success </span>
		</div>
		<div v-else-if="isLoggedOut">
			<LoginOptions />
		</div>
		<div v-else>
			<div v-if="nbLoginLoading > 0" class="d-flex align-items-center gap-2">
				<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>
				<span>Loading…</span>
			</div>
			<div v-else>?</div>
		</div> `,
};
