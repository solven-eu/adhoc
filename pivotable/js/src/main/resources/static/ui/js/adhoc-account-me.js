import { mapState } from "pinia";
import { useUserStore } from "./store-user.js";

import LoginChip from "./login-chip.js";
import AdhocMeLoggedIn from "./adhoc-account-me-loggedin.js";

export default {
	components: {
		LoginChip,
		AdhocMeLoggedIn,
	},
	computed: {
		...mapState(useUserStore, ["isLoggedIn"]),
	},
	setup() {
		return {};
	},
	template: /* HTML */ `
		<div v-if="!isLoggedIn"><LoginChip /></div>
		<div v-else>
			<AdhocMeLoggedIn />
		</div>
	`,
};
