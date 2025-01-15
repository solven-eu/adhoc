import { watch } from "vue";

import { mapState } from "pinia";
import { useKumiteStore } from "./store.js";
import { useUserStore } from "./store-user.js";

import KumiteNavbar from "./kumite-navbar.js";

import LoginRef from "./login-ref.js";

import KumiteAccountRef from "./kumite-account-ref.js";
import KumitePlayerRef from "./kumite-player-ref.js";

import LoginModal from "./login-modal.js";

export default {
	components: {
		KumiteNavbar,
		LoginRef,
		LoginModal,
		KumiteAccountRef,
		KumitePlayerRef,
	},
	computed: {
		...mapState(useUserStore, ["account", "tokens", "nbAccountFetching", "playingPlayerId"]),
	},
	setup() {
		const store = useKumiteStore();
		const userStore = useUserStore();

		// https://pinia.vuejs.org/core-concepts/state.html
		// Bottom of the page: there is a snippet for automatic persistence in localStorage
		// We still need to reload from localStorage on boot
		watch(
			userStore.$state,
			(state) => {
				// persist the whole state to the local storage whenever it changes
				localStorage.setItem("kumiteState", JSON.stringify(state));
			},
			{ deep: true },
		);

		// Load the metadata once and for all
		store.loadMetadata();

		// We may not be logged-in
		userStore
			.loadUser()
			.then(() => {
				return userStore.loadUserTokens();
			})
			.catch((error) => {
				userStore.onSwallowedError(error);
			});

		return {};
	},
	template: /* HTML */ `
        <div class="container">
            <KumiteNavbar />

            <main>
                <RouterView />
            </main>

            <LoginModal />
            <span v-if="$route.fullPath !== '/html/login'">
                <!--LoginRef /-->
            </span>

            <div v-else>
                <ul>
                    <li v-if="account.accountId"><KumiteAccountRef :accountId="account.accountId" /></li>
                    <li v-if="!!playingPlayerId"><KumitePlayerRef :playerId="playingPlayerId" /></li>
                </ul>
            </div>
        </div>
    `,
};