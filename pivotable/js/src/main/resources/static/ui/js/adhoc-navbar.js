import { ref } from "vue";
import { mapState } from "pinia";
import { useUserStore } from "./store-user.js";

import Logout from "./login-logout.js";
import PreferencesModal from "./adhoc-preferences-modal.js";

export default {
	components: {
		Logout,
		PreferencesModal,
	},
	computed: {
		...mapState(useUserStore, ["isLoggedIn", "account", "tokens", "nbLoginLoading", "needsToLogin", "needsToRefreshAccessToken"]),
	},
	setup() {
		const userStore = useUserStore();

		userStore.initializeUser();

		const expiresIn = ref("?");
		// In-flight flag for a user-triggered token refresh. Scoped to the navbar so the
		// clock pill can show a spinner + disable re-clicks without coupling back into the
		// store's general `nbLoginLoading` counter (which is incremented by many unrelated
		// calls and would cause the pill to flicker on any network activity).
		const refreshing = ref(false);

		// True once the token's `expires_at` is in the past — flips the pill copy from
		// "expires in X" to "expired since X" so the user knows clicking will rescue them
		// rather than just shorten a future expiry.
		const expired = ref(false);

		// Format a positive number of seconds with an adaptive unit so the readout stays
		// legible across token lifetimes ranging from a few seconds (e2e short-token profile,
		// PT3S) to a year (refresh_token default, P365D). Thresholds use 120 rather than 60
		// so we don't flicker "1 minute" / "59 seconds" around the boundary.
		const formatSeconds = function (s) {
			if (s < 120) {
				return s + " seconds";
			}
			if (s < 120 * 60) {
				return Math.round(s / 60) + " minutes";
			}
			if (s < 48 * 3600) {
				return Math.round(s / 3600) + " hours";
			}
			return Math.round(s / 86400) + " days";
		};

		// TODO Watch for initial tokens
		const updateClock = function () {
			if (!userStore.tokens.expires_at) {
				expiresIn.value = "?";
				expired.value = false;
				return;
			}
			const deltaSeconds = Math.round((userStore.tokens.expires_at - new Date()) / 1000);
			if (deltaSeconds >= 0) {
				expired.value = false;
				expiresIn.value = formatSeconds(deltaSeconds);
			} else {
				// Token already expired — show how long ago. Keep the digit positive so the
				// adaptive-unit formatter stays simple; the "expired since" prefix carries the
				// negative semantics in the template.
				expired.value = true;
				expiresIn.value = formatSeconds(-deltaSeconds);
			}
		};

		// Tick every second so the seconds readout is actually live; the computation is
		// trivial and the template only re-renders when the string actually changes.
		// https://stackoverflow.com/questions/65817482/time-on-vue-js-refreshing-automatically
		setInterval(() => {
			updateClock();
		}, 1000);
		updateClock();

		// Manual token refresh, triggered by clicking the countdown pill. `forceLoadUserTokens`
		// re-fetches the OAuth2 token bundle (new access_token + expiry) via the user's existing
		// SESSION cookie; no credential prompt is shown. We gate on `refreshing` so a rapid
		// double-click does not fire two overlapping refreshes.
		const refreshAccessToken = async function () {
			if (refreshing.value) return;
			refreshing.value = true;
			try {
				await userStore.forceLoadUserTokens();
				updateClock();
			} finally {
				refreshing.value = false;
			}
		};

		return { expiresIn, expired, refreshing, refreshAccessToken };
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
					<!--
						Logged-in user pill. Replaces an earlier flat layout that just dumped
						the name + avatar + logout button next to each other (it was unclear
						the name was the user.name). Now: a Bootstrap dropdown whose toggle
						shows "Logged in as <name>", with the avatar to its left; the menu
						opens to the user's full username, an account link, and a logout.
					-->
					<div v-if="isLoggedIn" class="dropdown">
						<button
							class="btn btn-link dropdown-toggle d-flex align-items-center gap-2 text-decoration-none"
							type="button"
							data-bs-toggle="dropdown"
							aria-expanded="false"
						>
							<img v-if="account.details.picture" :src="account.details.picture" class="rounded-circle" alt="" width="28" height="28" />
							<i v-else class="bi bi-person-circle fs-5"></i>
							<span class="small">Logged in as <strong>{{account.details.name || account.details.username}}</strong></span>
						</button>
						<ul class="dropdown-menu dropdown-menu-end">
							<li class="dropdown-header" v-if="account.details.username &amp;&amp; account.details.name">
								<small class="text-muted">{{account.details.username}}</small>
							</li>
							<li><hr class="dropdown-divider" v-if="account.details.username &amp;&amp; account.details.name" /></li>
							<li><Logout /></li>
						</ul>
					</div>
					<!--
						Session status indicator. The raw needsToLogin / needsToRefreshAccessToken
						values used to be dumped verbatim next to the user block — useful for
						debugging but noisy for end-users. We now expose only:
							- a small countdown until the access-token expires (the one useful bit);
							- a muted warning badge when the token needs a refresh;
							- a strong warning badge when the user must log in again.
						Everything is muted / small so the nav stays compact.

						The countdown is a clickable button: click it to manually refresh the
						access_token. While in-flight it shows a spinner-border-sm and is
						disabled, matching the project-wide async-UX rule in CLAUDE.md.
					-->
					<button
						v-if="isLoggedIn"
						type="button"
						class="btn btn-link btn-sm small ms-auto text-decoration-none p-0"
						:class="expired &amp;&amp; !refreshing ? 'text-danger' : 'text-muted'"
						:title="refreshing ? 'Refreshing access token…' : (expired ? 'Access token expired ' + expiresIn + ' ago — click to refresh now' : 'Access token expires in ' + expiresIn + ' — click to refresh now')"
						@click="refreshAccessToken"
						:disabled="refreshing"
					>
						<span v-if="refreshing" class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>
						<i v-else-if="expired" class="bi bi-exclamation-triangle me-1"></i>
						<i v-else class="bi bi-clock-history me-1"></i>
						<span v-if="refreshing">refreshing…</span>
						<span v-else-if="expired">expired since {{expiresIn}}</span>
						<span v-else>expires in {{expiresIn}}</span>
					</button>
					<span v-if="needsToLogin" class="badge rounded-pill text-bg-warning ms-2" title="Session expired — please log in again">
						Login required
					</span>
					<span
						v-else-if="isLoggedIn &amp;&amp; needsToRefreshAccessToken"
						class="badge rounded-pill text-bg-secondary ms-2"
						title="The access token is stale — a refresh will be issued on the next call"
					>
						refresh pending
					</span>
					<button
						type="button"
						class="btn btn-link btn-sm text-muted ms-2 p-0"
						title="Preferences"
						data-bs-toggle="modal"
						data-bs-target="#preferencesModal"
					>
						<i class="bi bi-sliders"></i>
					</button>
				</div>
			</div>
			<PreferencesModal />
		</nav>
	`,
};
