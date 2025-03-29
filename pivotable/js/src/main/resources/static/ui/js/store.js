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

		// The loaded endpoints and schemas
		endpoints: {},
		schemas: {},
		nbSchemaFetching: 0,
		columns: {},
		nbColumnFetching: 0,

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

		async loadEndpoints() {
			const store = this;

			async function fetchFromUrl(url) {
				store.nbSchemaFetching++;

				try {
					const response = await store.authenticatedFetch(url);
					if (!response.ok) {
						throw new Error("Rejected request for endpoints url" + url);
					}
					const responseJson = await response.json();

					responseJson.forEach((item) => {
						console.log("Registering endpointId", item.id);
						store.$patch({
							endpoints: { ...store.endpoints, [item.id]: item },
						});
					});
				} catch (e) {
					store.onSwallowedError(e);
				} finally {
					store.nbSchemaFetching--;
				}
			}

			return fetchFromUrl("/endpoints");
		},

		async loadEndpoint(endpointId) {
			console.log("About to load endpointId", endpointId);

			const store = this;

			async function fetchFromUrl(url) {
				store.nbSchemaFetching++;
				try {
					const response = await store.authenticatedFetch(url);
					if (!response.ok) {
						throw new Error("Rejected request for endpointId=" + endpointId);
					}

					const responseJson = await response.json();

					let endpoint;
					if (responseJson.length === 0) {
						// the endpointId does not exist
						endpoint = { error: "unknown" };
					} else if (responseJson.length !== 1) {
						throw new NetworkError("We expected a single endpoint", url, response);
					} else {
						endpoint = responseJson[0];
					}

					// https://github.com/vuejs/pinia/discussions/440
					console.log("Registering endpointId", endpointId);
					store.$patch({
						endpoints: { ...store.endpoints, [endpointId]: endpoint },
					});

					return endpoint;
				} catch (e) {
					store.onSwallowedError(e);

					const endpoint = {
						endpointId: endpointId,
						error: e,
					};
					store.$patch({
						endpoints: { ...store.endpoints, [endpointId]: endpoint },
					});

					return endpoint;
				} finally {
					store.nbSchemaFetching--;
				}
			}
			return fetchFromUrl(`/endpoints?endpoint_id=${endpointId}`);
		},

		async loadEndpointIfMissing(endpointId) {
			if (this.endpoints[endpointId]) {
				console.debug("Skip loading endpointId=", endpointId);
				return Promise.resolve(this.endpoints[endpointId]);
			} else {
				return this.loadEndpoint(endpointId);
			}
		},

		async loadEndpointSchemas(endpointId) {
			const store = this;

			async function fetchFromUrl(url) {
				store.nbSchemaFetching++;
				try {
					const response = await store.authenticatedFetch(url);
					const responseJson = await response.json();

					console.debug("responseJson", responseJson);

					const schemas = responseJson;
					schemas.forEach((schemaAndEndpoint) => {
						console.log("Registering schemaId", schemaAndEndpoint.endpoint.id);

						store.$patch({
							schemas: {
								...store.schemas,
								[schemaAndEndpoint.endpoint.id]: schemaAndEndpoint.schema,
							},
						});
					});
					return store.schemas;
				} catch (e) {
					store.onSwallowedError(e);
					return {};
				} finally {
					store.nbSchemaFetching--;
				}
			}

			let url = "/endpoints/schemas";
			if (endpointId) {
				// The schemas of a specific endpoint
				url += "?endpoint_id=" + encodeURIComponent(endpointId);
			}
			return this.loadEndpointIfMissing(endpointId).then(() => {
				return fetchFromUrl(url);
			});
		},

		async loadEndpointSchemaIfMissing(endpointId) {
			if (this.schemas[endpointId]) {
				console.debug("Skip loading schema for endpointId=", endpointId);
				return Promise.resolve(this.schemas[endpointId]);
			} else {
				return this.loadEndpointSchemas(endpointId).then((schemas) => {
					return schemas[endpointId];
				});
			}
		},

		async loadCubeSchema(cubeId, endpointId) {
			return this.loadEndpointSchemas(endpointId).then((schemas) => {
				if (schemas.length == 0) {
					const schema = {
						endpointId: endpointId,
						error: "None matching",
						cubes: [
							{
								cubeId: cubeId,
								error: "None matching",
							},
						],
					};

					return schema;
				} else {
					return schemas[endpointId]?.cubes[cubeId];
				}
			});
		},

		async loadCubeSchemaIfMissing(cubeId, endpointId) {
			const store = this;
			return this.loadEndpointIfMissing(endpointId).then(() => {
				if (store.schemas[endpointId]?.cubes[cubeId]) {
					console.debug("Skip loading cubeId=", cubeId);
					return Promise.resolve(store.schemas[endpointId]?.cubes[cubeId]);
				} else {
					return this.loadCubeSchema(cubeId, endpointId);
				}
			});
		},

		async loadColumnCoordinates(cubeId, endpointId, column) {
			const store = this;

			async function fetchFromUrl(url) {
				store.nbColumnFetching++;
				try {
					const response = await store.authenticatedFetch(url);
					const responseJson = await response.json();

					console.debug("responseJson", responseJson);

					const columns = responseJson;

					columns.forEach((columnJson) => {
						const columnId = `${endpointId}-${cubeId}-${column}`;
						console.log("Registering column", columnId, columnJson);

						store.$patch({
							columns: {
								...store.columns,
								[columnId]: columnJson,
							},
						});
					});
					return columns[0];
				} catch (e) {
					store.onSwallowedError(e);
					return {};
				} finally {
					store.nbColumnFetching--;
				}
			}

			let url = "/endpoints/schemas/columns";

			url += "?endpoint_id=" + encodeURIComponent(endpointId);
			url += "&cube=" + encodeURIComponent(cubeId);
			url += "&name=" + encodeURIComponent(column);

			return this.loadEndpointIfMissing(endpointId).then(() => {
				return fetchFromUrl(url);
			});
		},

		async loadColumnCoordinatesIfMissing(cubeId, endpointId, column) {
			const columnId = `${endpointId}-${cubeId}-${column}`;
			if (this.columns[columnId]) {
				console.debug("Skip loading columnId=", columnId);
				return Promise.resolve(this.columns[columnId]);
			} else {
				return this.loadColumnCoordinates(cubeId, endpointId, column);
			}
		},
	},
});
