// @ts-check
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

// Pending auto-retry handle for `loadMetadata`. Module-level instead of in the store state so the
// pinia state stays JSON-serializable (it is persisted to localStorage in some setups) and the
// TypeScript-via-JSDoc gate stays clean — `setTimeout` returns `number | NodeJS.Timeout`, which
// is awkward to type alongside the rest of the AdhocStoreState shape.
/** @type {ReturnType<typeof setTimeout> | null} */
let metadataRetryTimer = null;

/**
 * Capped exponential backoff schedule used when `loadMetadata` keeps failing. Exported as a pure
 * function so the policy can be unit-tested without spinning up pinia, fetch or timers.
 *
 * @param {number} attempt 1-indexed attempt number (the count after which the failure happened).
 * @returns {number} delay in ms to wait before the next attempt — 2s, 5s, 10s, 30s, then 60s cap.
 */
export function backoffDelayMs(attempt) {
	const schedule = [2000, 5000, 10000, 30000, 60000];
	const idx = Math.max(0, Math.min(attempt - 1, schedule.length - 1));
	return schedule[idx];
}

/**
 * @typedef BackendStatus
 * @property {"idle"|"loading"|"ok"|"error"} phase last observed phase of the public-metadata probe.
 * `idle` until the first call, then `loading`/`ok`/`error`. Auto-retries on `error` (see `loadMetadata`).
 * @property {number|null} http HTTP status (e.g. 502) when phase is `error` and the failure produced a response; null on network-layer failures.
 * @property {string|null} message human-readable description of the failure ("Bad Gateway", "Failed to fetch"); null when phase is `ok`/`idle`/`loading`.
 * @property {number} attempt number of attempts so far (including the in-flight or last one) — drives backoff.
 * @property {number|null} retryAt epoch-ms at which the next auto-retry will fire (when phase is `error`); null otherwise.
 */

/**
 * @typedef AdhocStoreState
 * @property {Record<string, any>} metadata server-public metadata payload — populated by `loadMetadata`
 * @property {BackendStatus} backendStatus tracks the public-metadata health probe. Used by `BackendStatusBanner` to surface "backend unreachable" UI.
 * @property {Record<string, any>} accounts account-id → account record, lazily filled by `loadAccount`
 * @property {number} nbAccountFetching count of in-flight account loads (for spinners)
 * @property {Record<string, any>} endpoints endpoint-id → endpoint descriptor; grown by `loadEndpoints` / local-only registrations
 * @property {Record<string, any>} schemas endpoint-id → schema (cube tree); grown by `loadEndpointSchemas`
 * @property {Record<string, boolean>} schemasLoadedFull endpoint-id → true when the schema was last loaded WITHOUT a `cube=` filter (i.e. carries every cube). A partial load (e.g. opening `/cubes/{cubeId}/query` directly) leaves the entry false/missing, so the endpoints listing knows to refresh before rendering a misleading 1-cube list.
 * @property {number} nbSchemaFetching in-flight schema loads (for spinners)
 * @property {Record<string, any>} columns column-id → column-details; grown by `loadCubeColumnDetails` / `loadAllCubeColumnsCoordinates`
 * @property {number} nbColumnFetching in-flight column-detail loads
 * @property {{ nextQuery: number }} queries query-id counter container
 * @property {number} nbQueryFetching in-flight query loads
 *
 * <p>NOTE: a handful of component files (`adhoc-account-chip.js`, `adhoc-account.js`) reference
 * {@code store.players[this.playerId]} which is NOT a member of this state and is also not written by any
 * action — it's a pre-existing dead reference that the runtime evaluates to {@code undefined.players} (NPE)
 * the moment those components actually mount. Flagged as a separate bug; left out of the typedef so the
 * type-checker surfaces the issue once the pinia wildcard is dropped in step 2.
 */
export const useAdhocStore = defineStore("adhoc", {
	state: () =>
		/** @type {AdhocStoreState} */ ({
			// Various metadata to enrich the UX
			metadata: {},
			backendStatus: { phase: "idle", http: null, message: null, attempt: 0, retryAt: null },

			// May load other accounts, for multi-accounts scenarios (e.g. query sharing)
			accounts: {},
			nbAccountFetching: 0,

			// endpoints are the available servers. Typically loading `self`, which is the same endpoint than the one serving JS
			endpoints: {},
			// schemas are the cubes. They are grouped by endpoints, as multiple endpoints may have cubes with the same name
			// we should consider schema for a endpoint+cube only if the endpoint is properly loaded
			schemas: {},
			// Tracks which endpoint schemas were loaded WITHOUT a `cube=` filter. A direct
			// navigation to a query URL loads only that cube's slice of the schema; later
			// jumping to the endpoints listing would otherwise display a single cube and
			// look like the endpoint hosts only one cube. The listing consults this map and
			// triggers a full reload when the flag is false/missing.
			schemasLoadedFull: {},
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

			// Accepts the bytes-so-far counter as the first arg (call sites pass it explicitly), even though the body
			// also reads `currentDecodedBytes` from the closure — keep the signature aligned with the callers.
			const onProgress = function (/** @type {any} */ _currentBytes, /** @type {any} */ done, /** @type {any} */ percent) {
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
			const url = prefix + "/public/metadata";

			// Cancel a pending auto-retry — a fresh call supersedes it (e.g. manual retry click).
			if (metadataRetryTimer) {
				clearTimeout(metadataRetryTimer);
				metadataRetryTimer = null;
			}

			store.$patch((state) => {
				state.backendStatus.phase = "loading";
				state.backendStatus.attempt += 1;
				state.backendStatus.retryAt = null;
			});

			try {
				const response = await fetch(url);
				if (!response.ok) {
					// HTTP-layer failure (typically 502/503/504 when the backend is down or restarting).
					store.$patch((state) => {
						state.backendStatus.phase = "error";
						state.backendStatus.http = response.status;
						state.backendStatus.message = response.statusText || "HTTP " + response.status;
					});
					store._scheduleMetadataRetry();
					return;
				}

				const metadata = await response.json();
				store.$patch((state) => {
					state.metadata = metadata;
					state.backendStatus.phase = "ok";
					state.backendStatus.http = null;
					state.backendStatus.message = null;
					state.backendStatus.attempt = 0;
				});
			} catch (e) {
				// Network-layer failure (DNS, TCP, CORS, JSON parse). `fetch` throws TypeError here.
				store.$patch((state) => {
					state.backendStatus.phase = "error";
					state.backendStatus.http = null;
					state.backendStatus.message = (e && e.message) || "Network error";
				});
				store._scheduleMetadataRetry();
			}
		},

		/**
		 * Schedule the next auto-retry of `loadMetadata` using a capped exponential backoff
		 * (2s, 5s, 10s, 30s, then 60s for every subsequent attempt). Side-effect: writes the
		 * target epoch-ms into `backendStatus.retryAt` so the banner can render a countdown.
		 *
		 * <p>private: invoked only from the failure branches of `loadMetadata`.
		 */
		_scheduleMetadataRetry() {
			const store = this;
			const delayMs = backoffDelayMs(store.backendStatus.attempt);
			const retryAt = Date.now() + delayMs;

			store.$patch((state) => {
				state.backendStatus.retryAt = retryAt;
			});

			metadataRetryTimer = setTimeout(() => {
				metadataRetryTimer = null;
				store.loadMetadata();
			}, delayMs);
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
					const isFullLoad = !cubeId;
					schemas.forEach((schemaAndEndpoint) => {
						console.log("Registering schemaId", schemaAndEndpoint.endpoint.id);

						store.$patch((state) => {
							state.schemas[schemaAndEndpoint.endpoint.id] = schemaAndEndpoint.schema;
							// A full load (no cube filter) supersedes any prior partial entry; a
							// partial load preserves a `true` flag from a previous full load if
							// any (so a partial reload doesn't downgrade the cache).
							if (isFullLoad) {
								state.schemasLoadedFull[schemaAndEndpoint.endpoint.id] = true;
							}
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

			// Reload when: (a) the schema isn't loaded at all / errored, OR (b) only a
			// partial slice was loaded earlier (typically when the user landed directly on a
			// `/cubes/{cubeId}/query` URL — only that cube's slice is cached). Without (b),
			// visiting the endpoints listing after a deep-linked query showed just the one
			// cube, masking the rest of the schema.
			const isPartial = !store.schemasLoadedFull[endpointId];
			if (availableSchema.error || isPartial) {
				console.log("Loading schema. error=", availableSchema.error, "isPartial=", isPartial);
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

		// Bulk-fetch every column's CoordinatesSample for a cube in ONE round-trip. Calls
		// `/endpoints/schemas/columns` without a `name` parameter — the server iterates every column of the
		// cube AND, since the controller now uses ICubeWrapper.getCoordinates(Map, int), the engine answers
		// every cardinality in a single batched query (e.g. SELECT COUNT(DISTINCT col1), COUNT(DISTINCT
		// col2)... on the SQL side). Each returned ColumnStatistics is registered in `state.columns` keyed
		// by its own column name — so the per-column Estimate badges populate in one sweep.
		async loadAllCubeColumnsCoordinates(cubeId, endpointId) {
			const store = this;
			let url = "/endpoints/schemas/columns";
			url += "?endpoint_id=" + encodeURIComponent(endpointId);
			url += "&cube=" + encodeURIComponent(cubeId);

			return this.loadEndpointIfMissing(endpointId).then(async () => {
				store.nbColumnFetching++;
				try {
					const response = await store.authenticatedFetch(url);
					const responseJson = await response.json();
					console.debug("bulk-coordinates response", responseJson);
					if (!Array.isArray(responseJson)) {
						return [];
					}
					responseJson.forEach((columnJson) => {
						const columnName = columnJson.column;
						if (!columnName) {
							console.warn("Skipping bulk-coordinates entry without column name", columnJson);
							return;
						}
						const columnId = `${endpointId}-${cubeId}-${columnName}`;
						store.$patch((state) => {
							state.columns[columnId] = columnJson;
						});
					});
					return responseJson;
				} catch (e) {
					store.onSwallowedError(e);
					return [];
				} finally {
					store.nbColumnFetching--;
				}
			});
		},
	},
});
