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

		// endpoints are the available servers. Typically loading `self`, which is the same endpoint than the one serving JS
		endpoints: {},
		// schemas are the cubes. They are grouped by endpoints, as multiple endpoints may have cubes with the same name
		// we should consider schema for a endpoint+cube only if the endpoint is properly loaded
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

		async authenticatedFetchStream(url, fetchOptions) {
			const userStore = useUserStore();

			return userStore.authenticatedFetchStream(url, fetchOptions);
		},

		async toJSON(response, externalOnProgress) {
			if (!response.ok) {
				throw new Error("Response is KO: " + response);
			}
			
			const gzipRatio = 8;
			// https://github.com/facebook/zstd
			const zstdRatio = 4;

			function decompressedSize(headers) {
				let totalDecodedBytes = headers.get("content-length");
				if (totalDecodedBytes) {
					const contentEncoding = headers.get("content-encoding");
					if (contentEncoding === "gzip") {
						// Heuristic: gzip decompressed size is 6 times the compressed size
						// This can not be done backend size as it would sacrifices the streamed serialization (i.e. given a very large Object, Reactor+Jackson will stream its properties into JSON)
						totalDecodedBytes *= gzipRatio;
					} else if (contentEncoding === "zstd") {
						totalDecodedBytes *= zstdRatio;
					}
				}
				return totalDecodedBytes;
			}
			
			let success = true;
			const totalDecodedBytes = decompressedSize(response.headers);

			let currentDecodedBytes = 0;
			const reader = response.body.getReader();
			const decoder = new TextDecoder();
			let text = "";

			const onProgress = function (done, percent) {
				if (totalDecodedBytes != undefined) {
					console.log("download progress:", currentDecodedBytes, totalDecodedBytes, done, percent);
				} else {
					console.log("download progress:", currentDecodedBytes, ", unknown total", done, percent);
				}

				if (externalOnProgress) {
					externalOnProgress(currentDecodedBytes, done, percent);
				}
			};

			while (true) {
				try {
					const { value, done } = await reader.read();
					if (done) {
						onProgress(currentDecodedBytes, done, 1);
						break;
					} else {
						currentDecodedBytes += value.length;

						// If the content is encoded, we may have underestimated the size of the unencoded content
						// Hence, we need to cap the current size
						if (totalDecodedBytes && currentDecodedBytes > totalDecodedBytes) {
							console.log("underestimation estimated=", totalDecodedBytes, " currentDecodedBytes=", currentDecodedBytes);
						}
						const percent = Math.min(currentDecodedBytes / totalDecodedBytes, 0.95);
						onProgress(currentDecodedBytes, done, percent);
						text += decoder.decode(value, { stream: true });
					}
				} catch (error) {
					console.error("error:", error);
					success = false;
					break;
				}
			}
			text += decoder.decode();

			return JSON.parse(text);
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

				store.$patch((state) => {
					state.metadata = metadata;
				});
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
						store.$patch((state) => {
							state.accounts[account.accountId] = account;
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

		/**
		 * returns either a valid endpoint, or an object with an `error` key.
		 */
		getLoadedEndpoint(endpointId) {
			const store = this;

			if (!store.endpoints[endpointId]) {
				return { error: "endpoint_notloaded" };
				//			} else if (store.endpoints[endpointId].error) {
				//				return {'error': "endpoint_" + store.endpoints[endpointId].error};
				//			} else if (!store.schemas[endpointId]) {
				//				return {'error': "endpoint_notloaded"};
				//			} else if (store.schemas[endpointId].error) {
				//				return {'error': "endpoint_" + store.schemas[endpointId].error};
				//			} else if (!store.schemas[endpointId].cubes[cubeId]) {
				//				return {'error': "cube_notloaded"};
			} else {
				// May hold an error
				return store.endpoints[endpointId];
			}
		},

		// Load endpoints from self url `/endpoints`
		// TODO The User should be able to add endpoints manually
		async loadEndpoints() {
			console.log("About to load all endpoints");

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
						store.$patch((state) => {
							state.endpoints[item.id] = item;
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

					console.log("Registering endpointId", endpointId);
					store.$patch((state) => {
						state.endpoints[endpointId] = endpoint;
					});

					return endpoint;
				} catch (e) {
					store.onSwallowedError(e);

					const endpoint = {
						endpointId: endpointId,
						error: e,
					};
					store.$patch((state) => {
						state.endpoints[endpointId] = endpoint;
					});

					return endpoint;
				} finally {
					store.nbSchemaFetching--;
				}
			}
			return fetchFromUrl(`/endpoints?endpoint_id=${endpointId}`);
		},

		/**
		 * returns a promise with either a valid endpoint, or a loading endpoint
		 */
		async loadEndpointIfMissing(endpointId) {
			const store = this;
			const availableEndpoint = store.getLoadedEndpoint(endpointId);

			if (availableEndpoint.error) {
				console.log("Loading endpoint due to error=", availableEndpoint.error);
				return this.loadEndpoint(endpointId);
			} else {
				console.debug("Skip loading endpointId=", endpointId);
				return Promise.resolve(this.endpoints[endpointId]);
			}
		},

		/**
		 * @param {string} endpointId id of the requested endpoint.
		 * @param {string} cubeId [Optional] id the the requested cube.
		 */
		async loadEndpointSchemas(endpointId, cubeId, externalOnProgress) {
			const store = this;

			async function fetchFromUrl(url) {
				store.nbSchemaFetching++;
				try {
					const response = await store.authenticatedFetchStream(url);
					if (!response.ok) {
						throw new Error("Rejected request for endpointId=" + endpointId);
					}
					const responseJson = await store.toJSON(response, externalOnProgress);

					console.debug("responseJson", responseJson);

					const schemas = responseJson;
					schemas.forEach((schemaAndEndpoint) => {
						console.log("Registering schemaId", schemaAndEndpoint.endpoint.id);

						store.$patch((state) => {
							state.schemas[schemaAndEndpoint.endpoint.id] = schemaAndEndpoint.schema;
						});
					});
					return store.schemas;
				} catch (e) {
					if (endpointId) {
						store.$patch((state) => {
							state.schemas[endpointId] = { error: e };
						});
					}

					store.onSwallowedError(e);
					return {};
				} finally {
					store.nbSchemaFetching--;
				}
			}
			return this.loadEndpointIfMissing(endpointId).then(() => {
				let url = "/endpoints/schemas";
				if (endpointId) {
					// The schemas of a specific endpoint
					url += "?endpoint_id=" + encodeURIComponent(endpointId);
				}
				if (cubeId) {
					// Restrict the schema to given cube
					// TODO Dynamic leading policy for `?` or `&`
					url += "&cube=" + encodeURIComponent(cubeId);
				}

				return fetchFromUrl(url);
			});
		},

		/**
		 * returns either a valid cube, or an object with an `error` key.
		 */
		getLoadedSchema(endpointId) {
			const store = this;

			if (!store.schemas[endpointId]) {
				return { error: "endpoint_notloaded" };
				//			} else if (store.schemas[endpointId].error) {
				//				return {'error': "endpoint_" + store.schemas[endpointId].error};
				//			} else if (!store.schemas[endpointId].cubes[cubeId]) {
				//				return {'error': "cube_notloaded"};
			} else {
				// May hold an error
				return store.schemas[endpointId];
			}
		},

		async loadEndpointSchemaIfMissing(endpointId, onProgress) {
			const store = this;
			const availableSchema = store.getLoadedSchema(endpointId);

			if (availableSchema.error) {
				console.log("Loading schema due to error=", availableSchema.error);
				return this.loadEndpointSchemas(endpointId, null, onProgress).then(() => {
					return store.getLoadedSchema(endpointId);
				});
			} else {
				console.debug("Skip loading schema for endpointId=", endpointId);
				return Promise.resolve(availableSchema);
			}
		},

		/**
		 * returns either a valid cube, or an object with an `error` key.
		 */
		getLoadedCube(cubeId, endpointId) {
			const store = this;

			const availableEndpoint = store.getLoadedEndpoint(endpointId);
			const availableSchema = store.getLoadedSchema(endpointId);

			if (availableEndpoint.error) {
				return { error: "endpoint_" + availableEndpoint.error };
			} else if (availableSchema.error) {
				return { error: "schema_" + availableSchema.error };
			} else if (!availableSchema.cubes || !availableSchema.cubes[cubeId]) {
				return { error: "cube_notloaded" };
			} else {
				return availableSchema.cubes[cubeId];
			}
		},

		async loadCubeSchema(cubeId, endpointId) {
			const store = this;

			return store.loadEndpointSchemas(endpointId, cubeId).then((schemas) => {
				if (schemas.length == 0) {
					const cubes = [];
					if (cubeId) {
						cubes.push({
							cubeId: cubeId,
							error: "None matching",
						});
					}

					const schema = {
						endpointId: endpointId,
						error: "None matching",
						cubes: cubes,
					};

					return schema;
				} else {
					return store.getLoadedCube(cubeId, endpointId);
				}
			});
		},

		async loadCubeSchemaIfMissing(cubeId, endpointId) {
			const store = this;
			return this.loadEndpointIfMissing(endpointId).then(() => {
				const cube = store.getLoadedCube(cubeId, endpointId);
				if (cube.error) {
					console.info("Loading cube due to error=", cube.error);
					return this.loadCubeSchema(cubeId, endpointId);
				} else {
					console.debug("Skip loading cubeId=", cubeId);
					return Promise.resolve(cube);
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

						store.$patch((state) => {
							state.columns[columnId] = columnJson;
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
