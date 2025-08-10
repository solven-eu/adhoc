import { watch } from "vue";

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
	return "10000000-1000-4000-8000-100000000000".replace(/[018]/g, (c) => (+c ^ (crypto.getRandomValues(new Uint8Array(1))[0] & (15 >> (+c / 4)))).toString(16));
}

function duplicate(object) {
	return JSON.parse(JSON.stringify(object));
}

export const usePreferencesStore = defineStore("preferences", {
	state: () => ({
		// queryId->queryModel
		queryModels: {},
		// used to help the user loading previous views
		latestQueryIds: [],

		// id of the active query.
		// TODO Should this be rather a URL parameter? (with a getter and a setter)
		currentQueryId: undefined,
	}),
	getters: {
		isDraft: (store) => !store.currentQueryId,
	},
	actions: {
		hashQuery(queryModel) {
			return hash(queryModel);
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
		loadQuery(queryId) {
			const store = this;

			if (queryId) {
				// Mark the requested queryId as currentQueryId
				// store.registerLatestQueryId(queryId);
			} else {
				// Use currentQueryId as implicitely requested query
				queryId = store.currentQueryId;
			}

			const loadedQueryModel = queryHelper.makeQueryModel();

			if (!queryId) {
				// No queryId at all: let's start a queryModel from scratch
				return loadedQueryModel;
			}

			// Load a copy to prevent mutations from Wizard
			queryHelper.parsedJsonToQueryModel(duplicate(store.queryModels[queryId].queryModel), loadedQueryModel);

			return loadedQueryModel;
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
	},
});
