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

		const expiresIn = ref("?");

		// TODO Watch for initial tokens
		const updateClock = function () {
			if (!userStore.tokens.expires_at) {
				expiresIn.value = "?";
				return;
			}
			// Adaptive unit so the readout stays legible across token lifetimes ranging from
			// a few seconds (e2e short-token profile, PT3S) to a year (refresh_token default,
			// P365D). Thresholds use 120 rather than 60 so we don't flicker "1 minute" / "59
			// seconds" around the boundary.
			const deltaSeconds = Math.round((userStore.tokens.expires_at - new Date()) / 1000);
			if (deltaSeconds < 120) {
				expiresIn.value = deltaSeconds + " seconds";
			} else if (deltaSeconds < 120 * 60) {
				expiresIn.value = Math.round(deltaSeconds / 60) + " minutes";
			} else if (deltaSeconds < 48 * 3600) {
				expiresIn.value = Math.round(deltaSeconds / 3600) + " hours";
			} else {
				expiresIn.value = Math.round(deltaSeconds / 86400) + " days";
			}
		};

		// Tick every second so the seconds readout is actually live; the computation is
		// trivial and the template only re-renders when the string actually changes.
		// https://stackoverflow.com/questions/65817482/time-on-vue-js-refreshing-automatically
		setInterval(() => {
			updateClock();
		}, 1000);
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
					<!--
						Session status indicator. The raw needsToLogin / needsToRefreshAccessToken
						values used to be dumped verbatim next to the user block — useful for
						debugging but noisy for end-users. We now expose only:
							- a small countdown until the access-token expires (the one useful bit);
							- a muted warning badge when the token needs a refresh;
							- a strong warning badge when the user must log in again.
						Everything is muted / small so the nav stays compact.
					-->
					<span class="small text-muted ms-auto" :title="'Access token expires in ' + expiresIn">
						<i class="bi bi-clock-history me-1"></i>expires in {{expiresIn}}
					</span>
					<span v-if="needsToLogin" class="badge rounded-pill text-bg-warning ms-2" title="Session expired — please log in again">
						Login required
					</span>
					<span
						v-else-if="needsToRefreshAccessToken"
						class="badge rounded-pill text-bg-secondary ms-2"
						title="The access token is stale — a refresh will be issued on the next call"
					>
						refresh pending
					</span>
				</div>
			</div>
		</nav>
	`,
};
