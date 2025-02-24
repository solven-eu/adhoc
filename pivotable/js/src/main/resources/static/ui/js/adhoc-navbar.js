import { mapState } from "pinia";
import { useUserStore } from "./store-user.js";

import Logout from "./login-logout.js";

export default {
	components: {
		Logout,
	},
	computed: {
		...mapState(useUserStore, ["isLoggedIn", "account", "tokens", "nbAccountFetching"]),
	},
	setup() {
		const userStore = useUserStore();

		userStore.loadUser();

		return {};
	},
	template: /* HTML */ `
        <nav class="navbar navbar-expand-lg navbar-light bg-light">
            <div class="container-fluid">
                <RouterLink class="navbar-brand" to="/">Adhoc</RouterLink>
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
                            <RouterLink class="nav-link" to="/html/entrypoints"><i class="bi bi-puzzle" />Entrypoints</RouterLink>
                        </li>
                        <!--li class="nav-item">
                            <RouterLink class="nav-link" to="/html/entrypoints/xxx/schemas"><i class="bi bi-trophy" />Schemas</RouterLink>
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
                </div>
            </div>
        </nav>
    `,
};
