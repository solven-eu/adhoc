import { watch } from "vue";

import { defineStore } from "pinia";

class NetworkError extends Error {
	constructor(message, url, response) {
		super(message);
		this.name = this.constructor.name;

		this.url = url;
		this.response = response;
	}
}

class UserNeedsToLoginError extends Error {
	constructor(message) {
		super(message);
		this.name = this.constructor.name;
	}
}

const prefix = "/api/v1";

export const useUserStore = defineStore("user", {
	state: () => ({
		// Currently connected account
		account: { details: {} },
		tokens: {},

		// Used to know when a first `loadUser` has kicked-in
		// hasTriedLoadingUser: false,

		// Some very first check to know if we are potentially logged-in
		// (May check some Cookie or localStorage, or some API preferably returning 2XX even if not logged-in)
		needsToCheckLogin: true,

		// Typically turned to true by an `authenticatedFetch` while loggedOut
		// Means the User is doing a connected operation, while being logged out
		// Hence, will probably open the login modal
		needsToLogin: false,

		// We loads information about various accounts (e.g. current account, through contests and leaderboards)
		// Playing players are stores in contests
		nbAccountLoading: 0,
	}),
	getters: {
		// If true, we have an account details. We typically have a session. Hence we can logout.
		// BEWARE `store.account.details.username` is not null after a session expiry, but `needsToLogin` would turn to true
		// If false, we need to check `needsToCheckLogin && needsToLogin`
		isLoggedIn: (store) => !store.needsToLogin && store.account.details.username,
		isLoggedOut: (store) => {
			if (store.isLoggedIn) {
				// No need to login as we have an account (hence presumably relevant Cookies/tokens)
				return false;
			} else if (store.needsToCheckLogin) {
				// We need to check login: we are not clearly logged-out
				return false;
			}

			// Not logged-in and login-status is checked explicitly: we're definitely logged-out
			return true;
		},
		// Default headers: we authenticate ourselves
		apiHeaders: (store) => {
			if (store.needsToRefreshAccessToken) {
				// TODO Implement automated access_token refresh through Promise
				throw new Error("access_token is missing or expired");
			}
			return { Authorization: "Bearer " + store.tokens.access_token };
		},
		needsToRefreshAccessToken: (store) => {
			return !store.tokens.access_token || store.tokens.access_token_expired;
		},
	},
	actions: {
		// Typically useful when an error is wrapped in the store
		onSwallowedError(error) {
			if (error instanceof NetworkError) {
				console.warn("An NetworkError is not being rethrown", error, error.response.status);
			} else {
				console.error("An Error is not being rethrown", error);
			}
		},
		newNetworkError(msg, url, response) {
			return new NetworkError("Rejected request for endpoints url" + url, url, response);
		},

		async fetchCsrfToken() {
			// https://www.baeldung.com/spring-security-csrf
			// If we relied on Cookie, `.csrfTokenRepository(CookieEndpointCsrfTokenRepository.withHttpOnlyFalse())` we could get the csrfToken with:
			// const csrfToken = document.cookie.replace(/(?:(?:^|.*;\s*)XSRF-TOKEN\s*\=\s*([^;]*).*$)|^.*$/, '$1');

			const response = await fetch(`/api/login/v1/csrf`);
			if (!response.ok) {
				throw new Error("Rejected request for logout");
			}

			const json = await response.json();
			const csrfHeader = json.header;
			console.debug("csrf header", csrfHeader);

			const freshCrsfToken = response.headers.get(csrfHeader);
			if (!freshCrsfToken) {
				throw new Error("Invalid csrfToken");
			}
			console.debug("csrf", freshCrsfToken);

			return { header: csrfHeader, token: freshCrsfToken };
		},

		// The point of this method is to detect login, without any 401 call, hence without any error or exception
		async fetchLoginStatus() {
			const response = await fetch(`/api/login/v1/json`);
			if (!response.ok) {
				throw new Error("Rejected request for login.json");
			}

			const json = await response.json();

			const loginHttpStatus = json.login;
			console.log("login status", loginHttpStatus);

			return loginHttpStatus;
		},

		// This would not fail if the User needs to login.
		// Hence, this method does not turn `needsToLogin` to true
		// Callers would generally rely on `ensureUser()` which may turn `needsToLogin` to true
		async loadUser() {
			const store = this;

			async function fetchFromUrl(url) {
				let response;

				// The following block can fail if there is no network connection
				// (Are we sure? Where are the unitTests?)
				{
					store.nbAccountLoading++;
					try {
						// Rely on session for authentication
						response = await fetch(url);
					} finally {
						store.nbAccountLoading--;
					}
				}

				if (response.status === 401) {
					// `fetchLoginStatus` said we were logged-in. What does this case mean?
					throw new UserNeedsToLoginError("User needs to login");
				} else if (!response.ok) {
					// What is this scenario? EndpointInternalError?
					throw new NetworkError("Rejected request for endpoints url" + url, url, response);
				}

				// We can typically get a Network error while fetching the json
				const responseJson = await response.json();
				const user = responseJson;

				console.log("User is logged-in", user);

				return user;
			}

			return store
				.fetchLoginStatus()
				.then((loginHttpStatus) => {
					if (loginHttpStatus === 200) {
						// We are logged-in
						// BEWARE: Current login mechanism does not handle the period while fetching user details
						// `needsToCheckLogin` is false and `account.details.username` is empty, hence we are considered loggedOut

						return fetchFromUrl("/api/login/v1/user")
							.then((user) => {
								store.needsToCheckLogin = false;

								// Ability to fetch current user: we are fully logged-in
								// `isLoggedIn` is computed from this value
								store.$patch({ account: user, needsToLogin: false });

								return user;
							})
							.catch((e) => {
								// The `login` status is OK, but there is an issue while fetching user details
								// Issue loadingUser while we checked the browser is logged-in
								console.warn("User needs to login", e);

								const user = { error: e };
								// Turns `needsToLogin` to true as we're partially logged-in
								store.$patch({ account: user, needsToLogin: true });
								return user;
							});
					} else {
						// Typically happens on first visit
						console.info("User is not logged-in");
						store.needsToCheckLogin = false;

						// We are not logged-in at all: do NOT turn `needsToLogin` to true
						const e = new UserNeedsToLoginError("User needs to login");
						const user = { error: e };
						return user;
					}
				})
				.catch((e) => {
					console.warn("Issue while checking login status", e);
					store.needsToCheckLogin = true;
				});
		},

		/**
		 * Calls this method from any component needed to know about the User.
		 *
		 * Contrary to `loadUser`, this will not trigger the loading of login state and userInfo on each call. It is especially userful not to have a spike of `loadUser` at startup.
		 */
		async initializeUser() {
			if (this.initializeUserPromise) {
				console.log(`Skip initializeUser as hasTriedLoadingUser=${this.initializeUserPromise}`);
			} else {
				console.log(`Doing initializeUser as hasTriedLoadingUser=${this.initializeUserPromise}`);
				this.initializeUserPromise = this.loadUserIfMissing();
			}
			return this.initializeUserPromise;
		},

		// do not throw if not logged-in
		async loadUserIfMissing() {
			if (this.isLoggedIn) {
				// We have loaded a user: we assume it does not need to login
				return Promise.resolve(this.account);
			} else if (!this.isLoggedOut) {
				// We are not logged-out
				return this.loadUser();
			} else {
				// The user may not need to login: we're loading if missing
				return Promise.resolve({ error: "UserIsLoggedOut" });
			}
		},

		// @throws UserNeedsToLoginError if not logged-in
		async ensureUser() {
			return loadUserIfMissing().then((user) => {
				if (user.error) {
					// We are not logged-in
					this.needsToLogin = true;
					throw new UserNeedsToLoginError("User needs to login");
				}
			});
		},

		async loadUserTokens() {
			const store = this;

			async function fetchFromUrl(url) {
				store.nbAccountLoading++;
				try {
					// Rely on session for authentication
					const response = await fetch(url);
					if (response.status === 401) {
						// This call was done as the application believed we were logged-in
						// But we we receive a 401, it means we're not logged-in

						// This will update the logout status
						// store.loadUser();

						store.needsToLogin = true;
						throw new UserNeedsToLoginError("User needs to login");
					} else if (!response.ok) {
						throw new NetworkError("Rejected request for tokens", url, response);
					}

					const responseJson = await response.json();
					const tokens = responseJson;

					{
						tokens.access_token_expired = false;

						// https://stackoverflow.com/questions/7687884/add-10-seconds-to-a-date
						const expiresAt = new Date();
						expiresAt.setSeconds(expiresAt.getSeconds() + tokens.expires_in);

						tokens.expires_at = expiresAt;
					}

					console.log("Tokens are stored. Expires at", tokens.expires_at);
					store.$patch({ tokens: tokens });

					watch(
						() => store.tokens.access_token_expired,
						(access_token_expired) => {
							if (access_token_expired) {
								console.log("access_token is expired. Triggering loadUserTokens");
								store.loadUserTokens();
							}
						},
					);

					return tokens;
				} catch (e) {
					store.onSwallowedError(e);
					return { error: e };
				} finally {
					store.nbAccountLoading--;
				}
			}

			return this.loadUserIfMissing().then((user) => {
				if (store.isLoggedIn) {
					console.log("We do have a User. Let's fetch tokens", user);
					return fetchFromUrl(`/api/login/v1/oauth2/token`);
				} else {
					return { error: "not_logged_in" };
				}
			});
		},

		async loadIfMissingUserTokens() {
			if (
				// We do have an accessToken
				this.tokens.access_token &&
				// We did not detect an expiry (e.g. receiving a 401)
				!this.tokens.access_token_expired &&
				// The token should not expire within 15 seconds
				new Date() - this.tokens.expires_at > 15
			) {
				console.debug("Authenticated and a valid access_tokenTokens is stored", this.tokens.access_token);
			} else {
				await this.loadUserTokens();
			}

			return this.tokens;
		},
		async authenticatedFetch(url, fetchOptions) {
			if (url.startsWith("/api")) {
				throw new Error("Invalid URL as '/api' is added automatically");
			}

			// loading missing tokens will ensure login status
			await this.loadIfMissingUserTokens();

			if (this.isLoggedOut) {
				this.needsToLogin = true;
				throw new UserNeedsToLoginError("User needs to login");
			}

			const apiHeaders = this.apiHeaders;

			// fetchOptions are optional
			fetchOptions = fetchOptions || {};

			// https://stackoverflow.com/questions/171251/how-can-i-merge-properties-of-two-javascript-objects
			const mergeHeaders = Object.assign({}, apiHeaders, fetchOptions.headers || {});

			const mergedFetchOptions = Object.assign({ method: "GET" }, fetchOptions);
			mergedFetchOptions.headers = mergeHeaders;

			console.debug("->", mergedFetchOptions.method, url, mergedFetchOptions);

			return fetch(prefix + url, mergedFetchOptions)
				.then((response) => {
					console.debug("<-", mergedFetchOptions.method, url, mergedFetchOptions, response);

					if (response.status == 401) {
						console.log("The access_token is expired as we received a 401");
						this.tokens.access_token_expired = true;

						// TODO Implement an automated retry after updated userTokens
					} else if (!response.ok) {
						console.trace("StackTrace for !ok on", url);
					}

					return response;
				})
				.catch((e) => {
					throw e;
				});
		},
	},
});
