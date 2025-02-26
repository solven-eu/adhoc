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
		schemas: {},
		nbSchemaFetching: 0,

		queries: { nextQuery: 0 },
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
				store.nbSchemaFetching++;

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
					store.nbSchemaFetching--;
				}
			}

			return fetchFromUrl("/entrypoints");
		},

		async loadEntrypoint(entrypointId) {
			console.log("About to load entrypointId", entrypointId);

			const store = this;

			async function fetchFromUrl(url) {
				store.nbSchemaFetching++;
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
					store.nbSchemaFetching--;
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

		async loadEntrypointSchemas(entrypointId) {
			const store = this;

			async function fetchFromUrl(url) {
				store.nbContestFetching++;
				try {
					const response = await store.authenticatedFetch(url);
					const responseJson = await response.json();

					console.debug("responseJson", responseJson);

					const schemas = responseJson;
					schemas.forEach((schemaAndEntrypoint) => {
						console.log("Registering schemaId", schemaAndEntrypoint.entrypoint.id);
						store.$patch({
							schemas: {
								...store.schemas,
								[schemaAndEntrypoint.entrypoint.id]: schemaAndEntrypoint.schema,
							},
						});
					});
					return store.schemas;
				} catch (e) {
					store.onSwallowedError(e);
					return {};
				} finally {
					store.nbContestFetching--;
				}
			}

			let url = "/entrypoints/schemas";
			if (entrypointId) {
				// The schemas of a specific entrypoint
				url += "?entrypoint_id=" + entrypointId;
			}
			return this.loadEntrypointIfMissing(entrypointId).then(() => {
				return fetchFromUrl(url);
			});
		},

		async loadEntrypointSchemaIfMissing(entrypointId) {
			if (this.schemas[entrypointId]) {
				console.debug("Skip loading schema for entrypointId=", entrypointId);
				return Promise.resolve(this.schemas[entrypointId]);
			} else {
				return this.loadEntrypointSchemas(entrypointId).then((schemas) => {
					return schemas[entrypointId];
				});
			}
		},

		async loadCubeSchema(cubeId, entrypointId) {
			return this.loadEntrypointSchemas(entrypointId).then((schemas) => {
				if (schemas.length == 0) {
					const schema = {
						entrypointId: entrypointId,
						entrypointId: entrypointId,
						error: "None matching",
					};

					return schema;
				} else {
					return schemas[entrypointId]?.cubes[cubeId];
				}
			});
		},

		async loadCubeSchemaIfMissing(cubeId, entrypointId) {
			return this.loadEntrypointIfMissing(entrypointId).then(() => {
				if (this.schemas[cubeId]) {
					console.debug("Skip loading cubeId=", cubeId);
					return Promise.resolve(this.schemas[cubeId]);
				} else {
					return this.loadCubeSchema(cubeId, entrypointId);
				}
			});
		},

		async executeQuery(cubeId, entrypointId, query) {
			return this.loadCubeSchemaIfMissing(cubeId, entrypointId).then((contest) => {
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
	},
});
