import { defineStore } from "pinia";

import queryHelper from "./adhoc-query-helper.js";

// https://stackoverflow.com/questions/7616461/generate-a-hash-from-string-in-javascript
const hash = (string) => {
	let hash = 0;

	if (!(typeof string === "string")) {
		string = JSON.stringify(string);
	}
	for (const char of string) {
		hash = (hash << 5) - hash + char.charCodeAt(0);
		hash |= 0; // Constrain to 32bit integer
	}
	return hash;
};

// https://stackoverflow.com/questions/105034/how-do-i-create-a-guid-uuid
function uuidv4() {
	return "10000000-1000-4000-8000-100000000000".replace(/[018]/g, (c) =>
		(+c ^ (crypto.getRandomValues(new Uint8Array(1))[0] & (15 >> (+c / 4)))).toString(16),
	);
}

function duplicate(object) {
	return JSON.parse(JSON.stringify(object));
}

// Single localStorage key holding the full preferences payload as a versioned JSON object.
// Old (pre-2026-04-22) schema used two separate keys `adhoc.preferences.queryModels` and
// `adhoc.preferences.latestQueryIds`; we still read them on first hydration for backwards
// compatibility, then write under STORAGE_KEY from there on.
const STORAGE_KEY = "adhoc.preferences";
// Schema version. Strings in ISO-8601 date format (YYYY-MM-DD) sort lexicographically, which
// lets migrations compare versions with plain `<` / `>`. Bump this value whenever the payload
// shape changes and add a corresponding `if` block in `migrate()`.
const STORAGE_VERSION = "2026-04-22";
const LEGACY_KEY_MODELS = "adhoc.preferences.queryModels";
const LEGACY_KEY_LATEST = "adhoc.preferences.latestQueryIds";

// Build the on-disk payload from the current store state. Extra fields (version, savedAt) let
// future migrations identify the schema and drop old entries if needed.
function buildPayload(state) {
	return {
		version: STORAGE_VERSION,
		savedAt: new Date().toISOString(),
		queryModels: state.queryModels,
		latestQueryIds: state.latestQueryIds,
		currentQueryId: state.currentQueryId,
		wizardHidden: state.wizardHidden,
		localEndpoints: state.localEndpoints,
		wizardOpenAccordion: state.wizardOpenAccordion,
	};
}

// Convert any prior schema into the current one. Each schema bump gets its own `if` block so
// migrations compose — an old payload upgrades step by step until it reaches STORAGE_VERSION.
// Version format: ISO-8601 date string (`YYYY-MM-DD`). Lexicographic order matches chronology,
// so a simple `<` comparison is enough. Payloads with no version (or a legacy numeric version
// from the pre-2026-04-22 era) are treated as "oldest possible" and run through every block.
function migrate(payload) {
	let result = { ...payload };
	// Normalize legacy numeric / missing version to a pre-2026-04-22 sentinel date.
	let v = typeof result.version === "string" ? result.version : "0000-00-00";

	if (v < "2026-04-22") {
		// Pre-2026-04-22: either no version field, or the old numeric scheme. No structural
		// change — re-stamp with the new date version.
		v = "2026-04-22";
	}
	// Future: if (v < "2026-05-??") { ...transform...; v = "2026-05-??"; }

	result.version = STORAGE_VERSION;
	return result;
}

// Read both the current STORAGE_KEY and the legacy keys, prefer the new schema, and return a
// payload ready to be fed to `migrate(...)`. Returns null if nothing is stored.
function readStoredPayload() {
	try {
		const raw = localStorage.getItem(STORAGE_KEY);
		if (raw) {
			return JSON.parse(raw);
		}
	} catch (e) {
		console.warn("preferences: failed to parse", STORAGE_KEY, e);
	}

	// Legacy: two separate keys without a version wrapper.
	try {
		const queryModels = JSON.parse(localStorage.getItem(LEGACY_KEY_MODELS) || "null");
		const latestQueryIds = JSON.parse(localStorage.getItem(LEGACY_KEY_LATEST) || "null");
		if (queryModels || latestQueryIds) {
			// No version field — `migrate()` treats the missing version as pre-2026-04-22
			// and upgrades through every block.
			return {
				queryModels: queryModels || {},
				latestQueryIds: latestQueryIds || [],
			};
		}
	} catch (e) {
		console.warn("preferences: failed to parse legacy keys", e);
	}

	return null;
}

const store = defineStore("preferences", {
	state: () => ({
		// queryId->queryModel
		queryModels: {},
		// used to help the user loading previous views
		latestQueryIds: [],

		// id of the active query.
		// TODO Should this be rather a URL parameter? (with a getter and a setter)
		currentQueryId: undefined,

		// When true, the query view hides the left-column wizard and expands the grid to
		// full width. Persisted so the preference survives reloads. Toggled from a button
		// in the grid column header (see adhoc-query.js).
		wizardHidden: false,

		// User-registered Pivotable endpoints (in addition to the ones discovered via the
		// `/endpoints` server route). Each entry is `{id, name, url, host, port, prefix,
		// local: true}`. `local: true` marks them as locally-added so the rest of the app
		// can distinguish them from server-side ones. Persisted in the same payload as
		// favorites so they survive a reload — the typical use case is registering a peer
		// Pivotable server (e.g. one running locally on a different host:port).
		localEndpoints: {},

		// Id of the wizard accordion section the user last opened (one of `wizardColumns`,
		// `wizardMeasures`, `wizardCustoms`, `wizardOptions`) or empty string when all are
		// collapsed. Restored on F5 so the wizard re-opens to the same section the user
		// was working in. Persisted via the standard `$subscribe` path.
		wizardOpenAccordion: "",
	}),
	getters: {
		isDraft: (store) => !store.currentQueryId,
	},
	actions: {
		hashQuery(queryModel) {
			return hash(queryModel);
		},

		// Add a user-defined tag to a saved favorite. Tags are free-form strings, deduped
		// case-insensitively but stored in their original case so the UI shows them the
		// way the user typed. No server round-trip — fully local, persisted via the same
		// localStorage path as the queryModel itself.
		addTagToFavorite(queryId, tag) {
			const store = this;
			const cleaned = (tag || "").trim();
			if (!cleaned) return;
			const entry = store.queryModels[queryId];
			if (!entry) return;
			store.$patch((state) => {
				const e = state.queryModels[queryId];
				if (!Array.isArray(e.tags)) {
					e.tags = [];
				}
				const existing = e.tags.find((t) => t.toLowerCase() === cleaned.toLowerCase());
				if (!existing) {
					e.tags.push(cleaned);
				}
			});
		},

		// Build a normalised endpoint object from raw inputs. Exposed (a) so callers can
		// preview the synthesised URL before committing, and (b) so unit tests can exercise
		// the URL-building rules without touching the store.
		buildLocalEndpoint(input) {
			const host = (input && input.host ? String(input.host) : "").trim();
			if (!host) {
				throw new Error("Host is required");
			}
			// Default port matches the Pivotable backend's `:8080`.
			const port = input.port === undefined || input.port === null || input.port === "" ? 8080 : Number(input.port);
			if (!Number.isInteger(port) || port <= 0 || port > 65535) {
				throw new Error("Port must be an integer in [1, 65535]");
			}
			let prefix = (input.prefix !== undefined && input.prefix !== null ? String(input.prefix) : "").trim();
			// Normalise: leading slash optional on input, always present on output. Trailing
			// slash dropped so concatenation with sub-routes behaves predictably.
			if (prefix && !prefix.startsWith("/")) {
				prefix = "/" + prefix;
			}
			while (prefix.endsWith("/")) {
				prefix = prefix.slice(0, -1);
			}
			const url = "http://" + host + ":" + port + prefix;
			const name = (input.name && String(input.name).trim()) || host + ":" + port + (prefix || "");
			return {
				id: input.id || uuidv4(),
				name,
				host,
				port,
				prefix,
				url,
				local: true,
			};
		},

		// Register a user-supplied endpoint (typically a peer Pivotable server reachable
		// over the network). The synthesised entry survives a reload via the same
		// localStorage payload that holds favorites. No server round-trip — the entry
		// exists only client-side, surfaced in the endpoints list alongside the
		// server-discovered ones.
		addLocalEndpoint(input) {
			const endpoint = this.buildLocalEndpoint(input);
			this.$patch((state) => {
				state.localEndpoints[endpoint.id] = endpoint;
			});
			return endpoint;
		},

		removeLocalEndpoint(id) {
			if (!id) return;
			this.$patch((state) => {
				delete state.localEndpoints[id];
			});
		},

		// Remove a tag from a favorite. Case-insensitive match. No-op if the favorite or
		// the tag is missing — the caller is the UI, which can fire the action freely.
		removeTagFromFavorite(queryId, tag) {
			const store = this;
			const cleaned = (tag || "").trim().toLowerCase();
			if (!cleaned) return;
			const entry = store.queryModels[queryId];
			if (!entry || !Array.isArray(entry.tags)) return;
			store.$patch((state) => {
				state.queryModels[queryId].tags = state.queryModels[queryId].tags.filter((t) => t.toLowerCase() !== cleaned);
			});
		},

		registerLatestQueryId(queryId) {
			const store = this;

			// Remove previous occurences of given queryId
			// https://stackoverflow.com/questions/15995963/remove-array-element-on-condition
			var i = 0;
			// `-1` as there is no point in removing the last entry, else we may remove and add current queryId
			// which would be an issue as it may leed to an infinite event loop
			while (i < store.latestQueryIds.length - 1) {
				if (store.latestQueryIds[i] === queryId) {
					store.latestQueryIds.splice(i, 1);
				} else {
					++i;
				}
			}

			// Append as most recent
			if (store.currentQueryId !== queryId) {
				store.currentQueryId = queryId;
			}
			if (store.latestQueryIds.length == 0 || store.latestQueryIds[store.latestQueryIds.length - 1] !== queryId) {
				store.latestQueryIds.push(queryId);
			}
		},

		saveQuery(queryModel, name, optId) {
			const store = this;

			if (!optId) {
				optId = uuidv4();
			}
			if (!name) {
				// https://stackoverflow.com/questions/47349417/javascript-date-now-to-readable-format
				name = "Query saved at " + new Date().toUTCString();
			}

			// Save a copy to prevent mutations from Wizard
			queryModel = duplicate(queryHelper.queryModelToParsedJson(queryModel));

			store.$patch((state) => {
				// Mark given queryId as currentQueryId
				// store.registerLatestQueryId(optId);

				// Save the queryModel
				if (!state.queryModels[optId]) {
					state.queryModels[optId] = {};
				}
				state.queryModels[optId].queryModel = queryModel;
				state.queryModels[optId].hash = this.hashQuery(queryModel);
				state.queryModels[optId].name = name;
			});

			return optId;
		},
		loadQuery(queryId, queryModel, registerLatestQueryId) {
			const store = this;

			if (queryId) {
				// Mark the requested queryId as currentQueryId
				// store.registerLatestQueryId(queryId);
			} else {
				// Use currentQueryId as implicitely requested query
				queryId = store.currentQueryId;
			}

			if (registerLatestQueryId) {
				// Typically done inside `loadQuery` else, if it called manually, it may failed as called from an unmounted component, as queryModel changed
				store.registerLatestQueryId(queryId);
			}

			if (!queryModel) {
				// Typically happens for transient loads, like from hasUnsavedChanges
				queryModel = queryHelper.makeQueryModel();
			}

			if (!queryId) {
				// No queryId at all: let's start a queryModel from scratch
				throw new Error("No query is registered for queryId=" + queryId);
			}

			// Load a copy to prevent mutations from Wizard
			queryHelper.parsedJsonToQueryModel(duplicate(store.queryModels[queryId].queryModel), queryModel);
		},
		// return true if given queryModel differs from the one registered for active queryId. true if draft==true
		hasUnsavedChanges(queryModel) {
			const store = this;

			if (store.isDraft) {
				// Current query is not saved as it is a draft
				return true;
			}
			const savedQueryModel = store.loadQuery(store.currentQueryId);
			return JSON.stringify(savedQueryModel) !== JSON.stringify(queryModel);
		},

		// Returns the full preferences payload as a JSON string, ready for download / clipboard.
		// Uses the same versioned schema as the localStorage blob so a round-trip through
		// export→import is lossless even across browsers.
		exportFavorites() {
			return JSON.stringify(buildPayload(this.$state), null, 2);
		},

		// Parse a JSON string previously produced by `exportFavorites` (or any compatible
		// payload) and merge it into the current state. Conflict strategy:
		//   - `queryModels`: imported entries overwrite existing entries with the same id.
		//   - `latestQueryIds`: union, preserving the imported order first.
		//   - `currentQueryId`: overwritten only when the imported payload carries one.
		// Throws on unparseable input or unknown schema version that can't be migrated.
		importFavorites(jsonString) {
			let payload;
			try {
				payload = JSON.parse(jsonString);
			} catch (e) {
				throw new Error("Input is not valid JSON: " + e.message);
			}
			if (!payload || typeof payload !== "object") {
				throw new Error("Expected a JSON object payload");
			}
			const migrated = migrate(payload);
			const store = this;
			store.$patch((state) => {
				const incomingModels = migrated.queryModels || {};
				state.queryModels = { ...state.queryModels, ...incomingModels };
				const incomingLatest = migrated.latestQueryIds || [];
				const seen = new Set();
				const mergedLatest = [];
				for (const id of [...incomingLatest, ...state.latestQueryIds]) {
					if (!seen.has(id)) {
						seen.add(id);
						mergedLatest.push(id);
					}
				}
				state.latestQueryIds = mergedLatest;
				if (migrated.currentQueryId) {
					state.currentQueryId = migrated.currentQueryId;
				}
			});
		},
	},
});

export const usePreferencesStore = function () {
	const theStore = store();

	// Hydrate + wire persistence exactly once per store instance. Pinia memoizes the store
	// across calls, so the `__hydrated` guard prevents re-loading stale data on every call
	// (and re-installing redundant subscribers). Previously hydration ran on every call,
	// which silently overwrote in-memory edits with whatever was on disk.
	if (!theStore.__hydrated) {
		const raw = readStoredPayload();
		if (raw) {
			const migrated = migrate(raw);
			theStore.queryModels = migrated.queryModels || {};
			theStore.latestQueryIds = migrated.latestQueryIds || [];
			if (migrated.currentQueryId) {
				theStore.currentQueryId = migrated.currentQueryId;
			}
			if (typeof migrated.wizardHidden === "boolean") {
				theStore.wizardHidden = migrated.wizardHidden;
			}
			if (migrated.localEndpoints && typeof migrated.localEndpoints === "object") {
				theStore.localEndpoints = migrated.localEndpoints;
			}
			if (typeof migrated.wizardOpenAccordion === "string") {
				theStore.wizardOpenAccordion = migrated.wizardOpenAccordion;
			}
		}

		// Persist on every state change. Pinia's $subscribe fires for any mutation (including
		// $patch), so save+import+remove all get written automatically — no more per-component
		// watchers needed, no more F5-amnesia.
		//
		// `{ detached: true }` is REQUIRED. Without it, $subscribe is bound to the lifetime of
		// the calling component (the first one to import this store) and the subscription is
		// torn down when that component unmounts. Subsequent component mounts find
		// `__hydrated === true` and skip the re-registration — leaving the store with NO
		// persistence subscriber. The user's "Save favorite" then mutates in-memory state
		// fine, but nothing ever lands in localStorage. Detached subscriptions live for the
		// life of the page, which is what we want.
		theStore.$subscribe(
			(mutation, state) => {
				try {
					localStorage.setItem(STORAGE_KEY, JSON.stringify(buildPayload(state)));
				} catch (e) {
					console.warn("preferences: failed to persist", e);
				}
			},
			{ detached: true },
		);

		theStore.__hydrated = true;
	}

	return theStore;
};
