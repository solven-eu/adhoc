import { mapState } from "pinia";

import { useUserStore } from "./store-user.js";

import LoginRef from "./login-ref.js";

export default {
	components: {
		LoginRef,
	},
	computed: {
		...mapState(useUserStore, ["isLoggedIn"]),
	},
	setup() {
		return {};
	},
	template: /* HTML */ `
        <div v-if="!isLoggedIn"><LoginRef /></div>
        <span v-else>
            <ul>
                <li><RouterLink to="/html/servers">Browse through servers</RouterLink></li>
                <li><RouterLink to="/html/me">About me</RouterLink></li>
            </ul>
        </span>
    `,
};
