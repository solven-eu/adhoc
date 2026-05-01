import { mapState } from "pinia";

import { useUserStore } from "./store-user.js";

import LoginChip from "./login-chip.js";

export default {
	components: {
		LoginChip,
	},
	computed: {
		...mapState(useUserStore, ["isLoggedIn"]),
	},
	setup() {
		return {};
	},
	template: /* HTML */ `
		<div v-if="!isLoggedIn"><LoginChip /></div>
		<span v-else>
			<ul>
				<li>
					<RouterLink to="/html/endpoints">Browse through endpoints</RouterLink>
				</li>
				<li><RouterLink to="/html/me">About me</RouterLink></li>
			</ul>
		</span>
	`,
};
