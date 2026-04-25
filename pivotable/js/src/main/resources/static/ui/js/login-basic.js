import { ref } from "vue";

import { mapState } from "pinia";
import { useUserStore } from "./store-user.js";

import { useRouter } from "vue-router";
import Logout from "./login-logout.js";

export default {
	components: {
		Logout,
	},
	props: {
		logout: {
			type: String,
			required: false,
		},
		modal: {
			type: Boolean,
			default: false,
		},
	},
	computed: {
		...mapState(useUserStore, ["nbLoginLoading", "account", "isLoggedIn"]),
		...mapState(useUserStore, {
			user(store) {
				return store.account;
			},
		}),
	},
	setup(props) {
		const userStore = useUserStore();
		const router = useRouter();

		userStore.initializeUser();

		// Some default credentials for a fake user
		const username = ref("11111111-1111-1111-1111-000000000000");
		const password = ref("no_password");

		const isExecutingBasic = ref(false);

		const doLoginBasic = function () {
			console.info("Login BASIC");
			async function fetchFromUrl(url, csrfToken) {
				// https://stackoverflow.com/questions/60265617/how-do-you-include-a-csrf-token-in-a-vue-js-application-with-a-spring-boot-backe
				const headers = {
					[csrfToken.header]: csrfToken.token,
					// https://stackoverflow.com/questions/43842793/basic-authentication-with-fetch
					Authorization: "Basic " + btoa(username.value + ":" + password.value),
				};

				try {
					isExecutingBasic.value = true;
					const response = await fetch(url, {
						method: "POST",
						headers: headers,
					});
					if (!response.ok) {
						throw new Error("Rejected request for logout");
					}

					const json = await response.json();

					console.info("Logged-in with BASIC", json);

					// force loading updated user (given we have received a fresh session cookie)
					userStore.forceLoadUser().then(() => {
						// load tokens for current user
						userStore.forceLoadUserTokens();
					});

					if (props.modal) {
						// Do not redirect as we're in a modal, and we want to stay on current location
						// Though, some cookies has been update, enabling to get an access_token
					} else {
						const loginSuccessHtmlRoute = json.Location;
						router.push(loginSuccessHtmlRoute);
					}
				} catch (e) {
					console.error("Issue on Network: ", e);
				} finally {
					isExecutingBasic.value = false;
				}
			}

			userStore.fetchCsrfToken().then((csrfToken) => {
				fetchFromUrl(`/api/login/v1/basic`, csrfToken);
			});
		};

		return { username, password, doLoginBasic, isExecutingBasic };
	},
	template: /* HTML */ `
		<div v-if="isLoggedIn" class="d-flex align-items-center gap-2">
			<Logout />
			<small class="text-muted">BASIC session lasts 1 hour.</small>
		</div>
		<!--
			Stacked form layout — the inputs sit one above the other and the action button
			fills the row width below them. This replaces an input-group that strung the
			whole thing on one line and pushed the Login button to the right edge of the
			screen, which felt awkward (most click targets in this UI are on the left).
		-->
		<form v-else class="d-flex flex-column gap-2" :inert="isExecutingBasic || nbLoginLoading ? true : null" @submit.prevent="doLoginBasic">
			<div>
				<label for="loginBasicUsername" class="form-label small text-muted mb-1">Username</label>
				<input id="loginBasicUsername" type="text" class="form-control form-control-sm" autocomplete="username" v-model="username" />
			</div>
			<div>
				<label for="loginBasicPassword" class="form-label small text-muted mb-1">Password</label>
				<input id="loginBasicPassword" type="password" class="form-control form-control-sm" autocomplete="current-password" v-model="password" />
			</div>
			<button type="submit" class="btn btn-primary btn-sm w-100" :disabled="isExecutingBasic || nbLoginLoading">
				<span v-if="isExecutingBasic || nbLoginLoading" class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>
				<span v-if="isExecutingBasic || nbLoginLoading">Signing in…</span>
				<span v-else>Login</span>
			</button>
		</form>
	`,
};
