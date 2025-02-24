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
		// Some very first check to know if we are potentially logged-in
		// (May check some Cookie or localStorage, or some API preferably returning 2XX even if logged-in)
		needsToCheckLogin: true,

		// Typically turned to true by an `authenticatedFetch` while loggedOut
		expectedToBeLoggedIn: false,

		// We loads information about various accounts (e.g. current account, through contests and leaderboards)
		// Playing players are stores in contests
		nbAccountLoading: 0,
	}),
	getters: {
		// If true, we have an account details. Hence we can logout.
		// If false, we need to check `needsToCheckLogin`
		isLoggedIn: (store) => store.account.details.username,
		isLoggedOut: (store) => {
			if (store.isLoggedIn) {
				// No need to login as we have an account (hence presumably relevant Cookies/tokens)
				return false;
			} else if (store.needsToCheckLogin) {
				// We need to check login: we are not clearly logged-out
				return false;
			}

			// Not logged-in and login-status is checked explicitly
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
			return new NetworkError("Rejected request for entrypoints url" + url, url, response);
		},

		async fetchCsrfToken() {
			// https://www.baeldung.com/spring-security-csrf
			// If we relied on Cookie, `.csrfTokenRepository(CookieEntrypointCsrfTokenRepository.withHttpOnlyFalse())` we could get the csrfToken with:
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
			console.log("login", loginHttpStatus);

			return loginHttpStatus;
		},

		// This would not fail if the User needs to login.
		// Callers would generally rely on `ensureUser()`
		async loadUser() {
			const store = this;

			async function fetchFromUrl(url) {
				let response;

				// The following block can fail if there is no netwrk connection
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
					throw new UserNeedsToLoginError("User needs to login");
				} else if (!response.ok) {
					// What is this scenario? EntrypointInternalError?
					throw new NetworkError("Rejected request for entrypoints url" + url, url, response);
				}

				// We can typically get a Network error while fetching the json
				const responseJson = await response.json();
				const user = responseJson;

				console.log("User is logged-in", user);

				return user;
			}

			return store.fetchLoginStatus().then((loginHttpStatus) => {
				store.needsToCheckLogin = false;

				if (loginHttpStatus === 200) {
					// We are logged-in
					// BEWARE: Current login mechanism does not handle the period while fetching user details
					// `needsToCheckLogin` is false and `account.details.username` is empty, hence we are considered loggedOut

					return fetchFromUrl("/api/login/v1/user")
						.then((user) => {
							// `isLoggedIn` is computed from this value
							store.$patch({ account: user });

							return user;
						})
						.catch((e) => {
							// Issue loadingUser while we checked the browser is logged-in
							console.warn("User needs to login");

							const user = { error: e };
							store.$patch({ account: user });
							return user;
						});
				} else {
					// Typically happens on first visit
					console.info("User needs to login");

					// We are not logged-in
					const e = new UserNeedsToLoginError("User needs to login");
					const user = { error: e };
					return user;
				}
			});
		},

		async loadUserIfMissing() {
			if (this.isLoggedIn) {
				// We have loaded a user: we assume it does not need to login
				return Promise.resolve(this.account);
			} else if (!this.isLoggedOut) {
				// We are not logged-out
				return this.loadUser();
			} else {
				// return Promise.reject(new UserNeedsToLoginError("User needs to login"));
				return Promise.resolve({ error: "UserNeedsToLogin" });
			}
		},

		// @throws UserNeedsToLoginError if not logged-in
		async ensureUser() {
			return loadUserIfMissing().then((user) => {
				if (user.error) {
					// We are not logged-in
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
                        // This will update the logout status
                        store.loadUser();
                        throw new UserNeedsToLoginError("User needs to login");
                    } else if (!response.ok) {
						throw new NetworkError("Rejected request for tokens", url, response);
					}

					const responseJson = await response.json();
					const tokens = responseJson;

					{
						tokens.access_token_expired = false;
					}

					console.log("Tokens are stored");
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

			return this.loadUser().then((user) => {
				if (store.isLoggedIn) {
					console.log("We do have a User. Let's fetch tokens", user);
					return fetchFromUrl(`/api/login/v1/oauth2/token`);
				} else {
					return { error: "not_logged_in" };
				}
			});
		},

		async loadIfMissingUserTokens() {
			if (this.tokens.access_token && !this.tokens.access_token_expired) {
				console.debug("Authenticated and an access_tokenTokens is stored", this.tokens.access_token);
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
				this.expectedToBeLoggedIn = true;
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
