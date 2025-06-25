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
		...mapState(useUserStore, ["nbAccountFetching", "account", "isLoggedIn"]),
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

					console.info("Login BASIC", json);

					if (props.modal) {
						// Do not redirect as we're in a modal, and we want to stay on current location
						// Though, some cookies has been update, enabling to get an access_token

						// force loading updated user (givne we have received a fresh session cookie)
						userStore.loadUser().then(() => {
							// load tokens for current user
							userStore.loadUserTokens();
						});
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
        <span v-if="isLoggedIn"> <Logout /><small>BASIC session lasts 1hour.</small> </span>
        <span v-else>
            <form class="input-group mb-3" :inert="isExecutingBasic ? true : null">
                <input type="text" class="form-control" placeholder="Username" aria-label="Username" v-model="username" />
                <span class="input-group-text">:</span>
                <input type="text" class="form-control" placeholder="Password" aria-label="Password" v-model="password" />
                <button type="button" @click="doLoginBasic" class="btn btn-primary">Login fakeUser</button>
				
				<div class="spinner-border" role="status" v-if="isExecutingBasic">
				  <span class="visually-hidden">Loading...</span>
				</div>
            </form>
        </span>
    `,
};
