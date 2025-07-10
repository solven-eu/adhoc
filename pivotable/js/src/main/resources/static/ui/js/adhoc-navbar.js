import { ref } from "vue";
import { mapState } from "pinia";
import { useUserStore } from "./store-user.js";

import Logout from "./login-logout.js";

export default {
	components: {
		Logout,
	},
	computed: {
		...mapState(useUserStore, ["isLoggedIn", "account", "tokens", "nbLoginLoading", "needsToLogin", "needsToRefreshAccessToken"]),
	},
	setup() {
		const userStore = useUserStore();

		userStore.initializeUser();

		const expiresIn = ref(null);

		// TODO Watch for initial tokens
		const updateClock = function () {
			if (userStore.tokens.expires_at) {
				expiresIn.value = Math.round((userStore.tokens.expires_at - new Date()) / (60 * 1000));
			} else {
				expiresIn.value = "?";
			}
		};

		// https://stackoverflow.com/questions/65817482/time-on-vue-js-refreshing-automatically
		setInterval(() => {
			updateClock();
		}, 20 * 1000);
		updateClock();

		return { expiresIn };
	},
	template: /* HTML */ `
        <nav class="navbar navbar-expand-lg navbar-light bg-light">
            <div class="container-fluid">
                <RouterLink class="navbar-brand" to="/">Pivotable (Adhoc)</RouterLink>
                <button
                    class="navbar-toggler"
                    type="button"
                    data-bs-toggle="collapse"
                    data-bs-target="#navbarSupportedContent"
                    aria-controls="navbarSupportedContent"
                    aria-expanded="false"
                    aria-label="Toggle navigation"
                >
                    <span class="navbar-toggler-icon"></span>
                </button>
                <div class="collapse navbar-collapse" id="navbarSupportedContent">
                    <ul class="navbar-nav me-auto mb-2 mb-lg-0">
                        <li class="nav-item">
                            <RouterLink class="nav-link" to="/html/endpoints"><i class="bi bi-puzzle" />Endpoints</RouterLink>
                        </li>
                        <!--li class="nav-item">
                            <RouterLink class="nav-link" to="/html/endpoints/xxx/schemas"><i class="bi bi-trophy" />Schemas</RouterLink>
                        </li-->
                    </ul>
                    <span v-if="isLoggedIn">
                        {{account.details.name}}<img
                            :src="account.details.picture"
                            class="img-thumbnail"
                            alt="You're looking nice"
                            width="64"
                            height="64"
                            v-if="account.details.picture"
                        />
                        <Logout />
                    </span>
                    needsToLogin={{needsToLogin}} needsToRefreshAccessToken={{needsToRefreshAccessToken}} expires_in={{expiresIn}} minutes
                </div>
            </div>
        </nav>
    `,
};
