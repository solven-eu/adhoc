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
	template: /* HTML */ `
		<div class="container py-4" style="max-width: 32rem">
			<div v-if="isLoggedIn" class="card shadow-sm">
				<div class="card-body">
					<!--
						Header block (avatar + name + welcome) is centred for the visual
						"hello again" beat. Action options below it are LEFT-aligned because
						they're a bullet list — centred bullets read as oddly indented and
						make the list less scannable.
					-->
					<div class="text-center">
						<img v-if="user.details.picture" :src="user.details.picture" class="rounded-circle mb-3" alt="" width="64" height="64" />
						<h5 class="card-title mb-1">Welcome {{user.details.name || user.details.username}}</h5>
						<p class="card-subtitle text-muted small mb-3" v-if="user.details.username && user.details.name">{{user.details.username}}</p>
						<div v-if="hintLoginSuccess" class="alert alert-success py-2 small mb-3">You are now signed in.</div>
						<div v-if="hintLoggedOut" class="alert alert-info py-2 small mb-3">You have been signed out.</div>
					</div>
					<Whatnow />
					<div class="mt-3"><Logout /></div>
				</div>
			</div>
			<div v-else-if="isLoggedOut" class="card shadow-sm">
				<div class="card-body">
					<h5 class="card-title mb-3 text-center">Sign in to Pivotable</h5>
					<LoginOptions />
				</div>
			</div>
			<div v-else class="d-flex align-items-center justify-content-center gap-2 py-4 text-muted">
				<span v-if="nbLoginLoading > 0" class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>
				<span v-if="nbLoginLoading > 0">Checking your session…</span>
				<span v-else>Initialising…</span>
			</div>
		</div>
	`,
};
