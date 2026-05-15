// @ts-check
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
		const userStore = useUserStore();
		// Flip needsToLogin to true on click. The global LoginModal watcher (in login-modal.js) is bound
		// to this exact transition and opens the Bootstrap modal — no router push, no full reload. Same
		// effect as if any auth-required operation had failed and demanded a login. Idempotent: clicking
		// the chip a second time while the modal is already open just re-asserts the flag.
		const openLoginModal = () => {
			userStore.needsToLogin = true;
		};
		return { openLoginModal };
	},
	template: /* HTML */ `
		<span v-if="isLoggedIn" :hidden="!modal">You are logged in</span>
		<span v-else-if="isLoggedOut">
			<!--
				Replaces a RouterLink to /html/login. The route was rendering BASIC as a plain <a href>
				which triggered a full document load (visible as a brief "Loading Pivotable" splash).
				The modal path stays entirely in-SPA — no navigation, no splash.
			-->
			<a href="#" @click.prevent="openLoginModal"><i class="bi bi-person"></i> You need to login</a>
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
