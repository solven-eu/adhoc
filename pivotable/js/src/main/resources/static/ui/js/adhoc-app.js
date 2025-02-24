import { watch } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";
import { useUserStore } from "./store-user.js";

import AdhocNavbar from "./adhoc-navbar.js";

import LoginRef from "./login-ref.js";

import AdhocAccountRef from "./adhoc-account-ref.js";

import LoginModal from "./login-modal.js";

export default {
	components: {
		AdhocNavbar,
		LoginRef,
		LoginModal,
		AdhocAccountRef,
	},
	computed: {
		...mapState(useUserStore, ["account", "tokens", "nbAccountFetching"]),
	},
	setup() {
		const store = useAdhocStore();
		const userStore = useUserStore();

		// https://pinia.vuejs.org/core-concepts/state.html
		// Bottom of the page: there is a snippet for automatic persistence in localStorage
		// We still need to reload from localStorage on boot
		watch(
			userStore.$state,
			(state) => {
				// persist the whole state to the local storage whenever it changes
				localStorage.setItem("adhocState", JSON.stringify(state));
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
            <AdhocNavbar />

            <main>
                <RouterView />
            </main>

            <LoginModal />
            <span v-if="$route.fullPath !== '/html/login'">
                <!--LoginRef /-->
            </span>

            <div v-else>
                <ul>
                    <li v-if="account.accountId">
                        <AdhocAccountRef :accountId="account.accountId" />
                    </li>
                </ul>
            </div>
        </div>
    `,
};
