import { defineStore } from "pinia";

import { useUserStore } from "./store-user.js";

class NetworkError extends Error {
	constructor(message, url, response) {
		super(message);
		this.name = this.constructor.name;

		this.url = url;
		this.response = response;
	}
}

const prefix = "/api/v1";

export const useAdhocStore = defineStore("adhoc", {
	state: () => ({
		// Various metadata to enrich the UX
		metadata: {},

		// May load other accounts, for multi-accounts scenarios (e.g. query sharing)
		accounts: {},
		nbAccountFetching: 0,

		// The loaded entrypoints and schemas
		entrypoints: {},
		nbEntrypointFetching: 0,

		schemas: {},
		nbSchemaFetching: 0,

		queries: {},
		nbQueryFetching: 0,
	}),
	getters: {
		// isLoggedIn is often used when manipulating schemas
		isLoggedIn: () => {
			const userStore = useUserStore();
			return userStore.isLoggedIn;
		},
		// account is often used when manipulating schemas
		account: () => {
			const userStore = useUserStore();
			return userStore.account;
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
			return new NetworkError("Rejected request for url" + url, url, response);
		},

		async authenticatedFetch(url, fetchOptions) {
			const userStore = useUserStore();

			return userStore.authenticatedFetch(url, fetchOptions);
		},

		async loadMetadata() {
			const store = this;

			async function fetchFromUrl(url) {
				const response = await fetch(url);
				if (!response.ok) {
					throw new NetworkError("Rejected request for url" + url, url, response);
				}

				const responseJson = await response.json();
				const metadata = responseJson;

				store.$patch({ metadata: metadata });
			}

			return fetchFromUrl(prefix + "/public/metadata");
		},

		async loadAccount(accountId) {
			const store = this;

			async function fetchFromUrl(url) {
				store.nbAccountFetching++;
				try {
					const response = await store.authenticatedFetch(url);
					if (!response.ok) {
						throw new Error("Rejected request for accountId=" + accountId);
					}

					const responseJson = await response.json();
					const accounts = responseJson;

					accounts.forEach((account) => {
						console.log("Storing accountId", account.accountId);
						store.$patch({
							accounts: { ...store.accounts, [account.accountId]: account },
						});

						store.loadAccountIfMissing(account.playerId);
					});
				} catch (e) {
					store.onSwallowedError(e);
				} finally {
					store.nbAccountFetching--;
				}
			}

			return fetchFromUrl(`/accounts?account_id=${accountId}`);
		},

		async loadAccountIfMissing(accountId) {
			if (this.accounts[accountId]) {
				console.debug("Skip loading accountId=", accountId);
				return Promise.resolve(this.accounts[accountId]);
			} else {
				return this.loadAccount(accountId);
			}
		},

		async loadEntrypoints() {
			const store = this;

			async function fetchFromUrl(url) {
				store.nbEntrypointFetching++;

				try {
					const response = await store.authenticatedFetch(url);
					if (!response.ok) {
						throw new Error("Rejected request for entrypoints url" + url);
					}
					const responseJson = await response.json();

					responseJson.forEach((item) => {
						console.log("Registering entrypointId", item.id);
						store.$patch({
							entrypoints: { ...store.entrypoints, [item.id]: item },
						});
					});
				} catch (e) {
					store.onSwallowedError(e);
				} finally {
					store.nbEntrypointFetching--;
				}
			}

			return fetchFromUrl("/entrypoints");
		},

		async loadEntrypoint(entrypointId) {
			console.log("About to load entrypointId", entrypointId);

			const store = this;

			async function fetchFromUrl(url) {
				store.nbEntrypointFetching++;
				try {
					const response = await store.authenticatedFetch(url);
					if (!response.ok) {
						throw new Error("Rejected request for entrypointId=" + entrypointId);
					}

					const responseJson = await response.json();

					let entrypoint;
					if (responseJson.length === 0) {
						// the entrypointId does not exist
						entrypoint = { error: "unknown" };
					} else if (responseJson.length !== 1) {
						throw new NetworkError("We expected a single entrypoint", url, response);
					} else {
						entrypoint = responseJson[0];
					}

					// https://github.com/vuejs/pinia/discussions/440
					console.log("Registering entrypointId", entrypointId);
					store.$patch({
						entrypoints: { ...store.entrypoints, [entrypointId]: entrypoint },
					});

					return entrypoint;
				} catch (e) {
					store.onSwallowedError(e);

					const entrypoint = {
						entrypointId: entrypointId,
						error: e,
					};
					store.$patch({
						entrypoints: { ...store.entrypoints, [entrypointId]: entrypoint },
					});

					return entrypoint;
				} finally {
					store.nbEntrypointFetching--;
				}
			}
			return fetchFromUrl(`/entrypoints?entrypoint_id=${entrypointId}`);
		},

		async loadEntrypointIfMissing(entrypointId) {
			if (this.entrypoints[entrypointId]) {
				console.debug("Skip loading entrypointId=", entrypointId);
				return Promise.resolve(this.entrypoints[entrypointId]);
			} else {
				return this.loadEntrypoint(entrypointId);
			}
		},

		async loadSchemas(entrypointId) {
			const store = this;

			async function fetchFromUrl(url) {
				store.nbContestFetching++;
				try {
					const response = await store.authenticatedFetch(url);
					const responseJson = await response.json();

					console.debug("responseJson", responseJson);

					const schemas = responseJson;
					schemas.forEach((schema) => {
						console.log("Registering schemaId", schema.entrypoint.id);
						store.$patch({
							schemas: {
								...store.schemas,
								[schema.entrypoint.id]: schema,
							},
						});
					});
					return schemas;
				} catch (e) {
					store.onSwallowedError(e);
					return [];
				} finally {
					store.nbContestFetching--;
				}
			}

			let url = "/entrypoints/schemas";
			if (entrypointId) {
				// The schemas of a specific entrypoint
				url += "?entrypoint_id=" + entrypointId;
			}
			return fetchFromUrl(url);
		},

		mergeContest(contestUpdate) {
			const contestId = contestUpdate.contestId;
			// The contest may be empty on first load
			const oldContest = this.schemas[contestId] || {};
			// This this property right-away as it is watched
			const mergedContest = {
				...oldContest,
				...contestUpdate,
				stale: false,
			};

			// BEWARE This is broken if we consider a user can manage multiple playerIds
			console.log("Storing board for contestId", contestId, mergedContest);
			this.$patch({
				schemas: { ...this.schemas, [contestId]: mergedContest },
			});

			return mergedContest;
		},

		async loadSchema(contestId, entrypointId) {
			let entrypointPromise;
			if (entrypointId) {
				entrypointPromise = this.loadEntrypointIfMissing(entrypointId);
			} else {
				entrypointPromise = Promise.resolve();
			}

			return entrypointPromise.then(() => {
				console.log("About to load/refresh contestId", contestId);

				const store = this;

				async function fetchFromUrl(url) {
					store.nbContestFetching++;
					try {
						const response = await store.authenticatedFetch(url);
						if (!response.ok) {
							throw new NetworkError("Rejected request for contest: " + contestId, url, response);
						}

						const responseJson = await response.json();

						if (responseJson.length === 0) {
							return { contestId: contestId, error: "unknown" };
						} else if (responseJson.length !== 1) {
							// This should not happen as we provided an input contestId
							console.error("We expected a single contest", responseJson);
							return { contestId: contestId, error: "unknown" };
						}

						const contest = responseJson[0];

						return contest;
					} catch (e) {
						store.onSwallowedError(e);

						const contest = {
							contestId: contestId,
							error: e,
						};

						return contest;
					} finally {
						store.nbContestFetching--;
					}
				}
				return fetchFromUrl(`/schemas?contest_id=${contestId}`).then((contest) => {
					return this.mergeContest(contest);
				});
			});
		},

		async loadSchemaIfMissing(contestId, entrypointId) {
			let entrypointPromise;
			if (entrypointId) {
				entrypointPromise = this.loadEntrypointIfMissing(entrypointId);
			} else {
				entrypointPromise = Promise.resolve();
			}
			return entrypointPromise.then(() => {
				if (this.schemas[contestId]) {
					console.debug("Skip loading contestId=", contestId);
					return Promise.resolve(this.schemas[contestId]);
				} else {
					return this.loadSchema(contestId, entrypointId);
				}
			});
		},

		async loadBoard(entrypointId, contestId, playerId) {
			console.debug("entrypointId", entrypointId);
			if (!playerId) {
				playerId = useUserStore().playingPlayerId;
			}
			if (!playerId) {
				throw new Error("playingPlayerId is undefined");
			}

			const store = this;

			return this.loadSchemaIfMissing(contestId, entrypointId).then((contest) => {
				if (contest.error === "unknown") {
					return contest;
				}

				async function fetchFromUrl(url) {
					store.nbBoardFetching++;
					try {
						const response = await store.authenticatedFetch(url);
						if (!response.ok) {
							throw new NetworkError("Rejected request for board: " + contestId, url, response);
						}

						const responseJson = await response.json();
						const contestWithBoard = responseJson;

						return contestWithBoard;
					} catch (e) {
						store.onSwallowedError(e);

						return {
							contestId: contestId,
							error: e,
						};
					} finally {
						store.nbBoardFetching--;
					}
				}

				return fetchFromUrl(`/board?entrypoint_id=${entrypointId}&contest_id=${contestId}&player_id=${playerId}`).then((contestWithBoard) =>
					this.mergeContest(contestWithBoard),
				);
			});
		},

		async loadLeaderboard(entrypointId, contestId) {
			const store = this;

			async function fetchFromUrl(url) {
				try {
					const response = await store.authenticatedFetch(url);
					if (!response.ok) {
						throw new NetworkError("Rejected request for leaderboard: " + contestId, url, response);
					}

					const responseJson = await response.json();

					const leaderboard = responseJson;

					// We need to configure all object properties right-away
					// Else, `stale` would not be reset/removed by a fresh leaderboard (i.e. without `stale` property)
					// https://stackoverflow.com/questions/76709501/pinia-state-not-updating-when-using-spread-operator-object-in-patch
					// https://github.com/vuejs/pinia/issues/43
					leaderboard.stale = false;

					// https://github.com/vuejs/pinia/discussions/440
					console.log("Storing leaderboard for contestId", contestId);
					store.$patch({
						leaderboards: {
							...store.leaderboards,
							[contestId]: leaderboard,
						},
					});
				} catch (e) {
					store.onSwallowedError(e);

					const leaderboard = {
						contestId: contestId,
						error: e,
						stale: false,
					};
					store.$patch({
						leaderboards: {
							...store.leaderboards,
							[contestId]: leaderboard,
						},
					});
					return leaderboard;
				}
			}

			store.nbLeaderboardFetching++;
			return this.loadSchemaIfMissing(contestId, entrypointId)
				.then(() => fetchFromUrl("/leaderboards?contest_id=" + contestId))
				.finally(() => {
					store.nbLeaderboardFetching--;
				});
		},
	},
});
