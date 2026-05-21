// @ts-check
import wizardHelper from "./adhoc-query-wizard-helper.js";

export default {
	makeQueryModel: function () {
		const queryModel = {
			// `columnName->boolean`
			selectedColumns: {},
			// `orderedArray of columnNames`
			selectedColumnsOrdered: [],
			// `columnName->boolean`
			withStarColumns: {},
			// `measureName->boolean`
			selectedMeasures: {},
			filter: {},
			customMarkers: {},
			// `optionName->boolean`
			selectedOptions: {},
			// Pause/resume parity with filters. `disabledColumns[name] === true` and
			// `disabledMeasures[name] === true` mean: keep the entry in `selectedColumns` /
			// `selectedMeasures` (so the wizard pill, with its remove button, stays where the
			// user left it), but DROP it from the wire-level query at submit time. The user
			// re-enables with a single click on the same pause/resume toggle. Symmetrical with
			// `filter.disabled` on filter tree nodes.
			disabledColumns: {},
			disabledMeasures: {},
		};

		queryModel.reset = function () {
			// IMPORTANT: use `this` rather than the closure-captured `queryModel`.
			// `makeQueryModel()` returns the raw object, which the parent wraps in `reactive()`
			// before handing it around as a prop. The closure `queryModel` is the RAW target;
			// mutating through it does not go through the Proxy, so Vue never sees the changes
			// and the UI stays frozen. When this method is invoked as `proxy.reset()`, `this`
			// is the reactive proxy and mutations fire the expected effects.
			//
			// Additionally, we clear objects IN-PLACE (via `delete`) instead of reassigning
			// to fresh `{}`. `v-model="queryModel.selectedMeasures[name]"` checkboxes depend on
			// per-key reactivity of the nested proxy — replacing the parent reference leaves
			// those bindings unaffected in practice, whereas deleting each key explicitly
			// fires the per-key triggers and flips the switches.
			Object.keys(this.selectedColumns).forEach((k) => delete this.selectedColumns[k]);
			this.selectedColumnsOrdered.splice(0);
			// TODO withStarColumns may not be reset as they as some sort of preference
			// Still, they are resetted in this method as a way to ensure the model is not corrupted.
			Object.keys(this.withStarColumns).forEach((k) => delete this.withStarColumns[k]);
			Object.keys(this.selectedMeasures).forEach((k) => delete this.selectedMeasures[k]);
			Object.keys(this.filter).forEach((k) => delete this.filter[k]);
			Object.keys(this.customMarkers).forEach((k) => delete this.customMarkers[k]);
			Object.keys(this.selectedOptions).forEach((k) => delete this.selectedOptions[k]);
			if (this.disabledColumns) Object.keys(this.disabledColumns).forEach((k) => delete this.disabledColumns[k]);
			if (this.disabledMeasures) Object.keys(this.disabledMeasures).forEach((k) => delete this.disabledMeasures[k]);
			console.log("queryModel has been reset");
		};

		queryModel.copy = function () {
			const copied = { ...this };

			console.log(queryModel);

			copied.selectedColumns = JSON.parse(JSON.stringify(queryModel.selectedColumns));
			copied.selectedColumnsOrdered = JSON.parse(JSON.stringify(queryModel.selectedColumnsOrdered));
			copied.withStarColumns = JSON.parse(JSON.stringify(queryModel.withStarColumns));
			copied.selectedMeasures = JSON.parse(JSON.stringify(queryModel.selectedMeasures));
			copied.filter = JSON.parse(JSON.stringify(queryModel.filter));
			copied.selectedOptions = JSON.parse(JSON.stringify(queryModel.selectedOptions));
			copied.customMarkers = JSON.parse(JSON.stringify(queryModel.customMarkers));

			return copied;
		};

		queryModel.columns = function () {
			return wizardHelper.queried(queryModel.selectedColumns || {});
		};
		queryModel.measures = function () {
			return wizardHelper.queried(queryModel.selectedMeasures || {});
		};
		//		queryModel.filter = function () {
		//			return queryModel.filter || {};
		//		};
		queryModel.customMarker = function () {
			return queryModel.customMarkers || {};
		};
		queryModel.options = function () {
			return wizardHelper.queried(queryModel.selectedOptions || {});
		};

		queryModel.onColumnToggled = function (column) {
			const array = queryModel.selectedColumnsOrdered;

			if (!column) {
				// We lack knowledge about which columns has been toggled
				for (const column of Object.keys(queryModel.selectedColumns)) {
					const index = array.indexOf(column);

					let isChanged = false;

					// May be missing on first toggle
					const toggledIn = !!queryModel.selectedColumns[column];
					if (toggledIn) {
						if (index < 0) {
							// Append the column
							array.push(column);
							isChanged = true;
						}
					} else {
						// https://stackoverflow.com/questions/5767325/how-can-i-remove-a-specific-item-from-an-array-in-javascript
						// only splice array when item is found
						if (index >= 0) {
							// 2nd parameter means remove one item only
							array.splice(index, 1);
							isChanged = true;
						}
					}
					if (isChanged) {
						console.log(`groupBy: ${column} is now ${toggledIn}`);
					} else {
						console.debug(`groupBy: ${column} is kept ${toggledIn}`);
					}
				}
			} else {
				const index = array.indexOf(column);

				// May be missing on first toggle
				const toggledIn = !!queryModel.selectedColumns[column];
				if (toggledIn) {
					if (index < 0) {
						// Append the column
						array.push(column);
					} else {
						console.warn("Adding a column already here?", column);
					}
				} else {
					// https://stackoverflow.com/questions/5767325/how-can-i-remove-a-specific-item-from-an-array-in-javascript
					// only splice array when item is found
					if (index >= 0) {
						// 2nd parameter means remove one item only
						array.splice(index, 1);
					} else {
						console.warn("Removing a column already absent?", column);
					}
				}
				console.log(`groupBy: ${column} is now ${toggledIn}`);
			}
		};

		return queryModel;
	},

	/**
	 * Return the current URL hash, decoded, ready to feed to {@link this.hashToQueryModel}.
	 *
	 * <p>Why not use {@code router.currentRoute.value.hash}? The watcher in adhoc-query.js
	 * updates the URL on every model edit via {@code history.pushState(...)}, which BYPASSES
	 * vue-router. So vue-router's reactive {@code currentRoute.value.hash} desyncs from the
	 * real {@code window.location.hash} every time the user edits the model. On the
	 * remount path (after a token expiry + re-login swaps {@code <LoginChip>} back to
	 * {@code <AdhocQuery>}), vue-router reports its STALE last-known hash — often empty —
	 * and the queryModel is "restored" from nothing. {@code window.location.hash} is the
	 * authoritative source.
	 *
	 * <p>{@code window.location.hash} returns the URL-encoded form; vue-router
	 * decodes its own {@code currentRoute.value.hash}. We mirror the decoded shape so
	 * callers (notably {@link this.hashToQueryModel}) accept either source uniformly.
	 *
	 * @param {{ location?: { hash?: string } } | null | undefined} [windowLike] DI seam — defaults
	 *     to the real {@code window} when omitted. Tests pass a stub.
	 * @returns {string} the hash including the leading "#", or "" when no hash is present
	 */
	readUrlHash: function (windowLike) {
		const w = windowLike || (typeof window !== "undefined" ? window : null);
		const raw = w && w.location && w.location.hash;
		if (!raw || !raw.startsWith("#")) {
			return "";
		}
		try {
			return "#" + decodeURIComponent(raw.substring(1));
		} catch {
			// Malformed URI — fall back to the raw form; hashToQueryModel will try-catch JSON.parse.
			return raw;
		}
	},

	/**
	 * Current URL/storage encoding version for the queryModel hash payload.
	 *
	 * <p><b>Semantics — `v` is a BREAKING-CHANGE marker, not a feature flag.</b>
	 * Bump it only when the on-wire shape changes in a way an older reader could not handle
	 * (e.g. renaming {@code measures} → {@code m}, restructuring {@code filter}, dropping a
	 * required field). Adding a new OPTIONAL field with a sensible default does NOT warrant
	 * a bump — older readers ignore it, newer readers treat absence as default.
	 *
	 * <p>Each bump requires {@link #hashToQueryModel} to either understand both shapes (one-step
	 * migration in-place) or refuse the unknown shape cleanly via the "unknown v" branch.
	 *
	 * @type {number}
	 */
	URL_HASH_VERSION: 1,

	hashToQueryModel: function (currentHashDecoded, queryModel) {
		// Restore queryModel from URL hash
		if (currentHashDecoded && currentHashDecoded.startsWith("#")) {
			try {
				const currentHashObject = JSON.parse(currentHashDecoded.substring(1));

				// `v` is the breaking-change marker; see URL_HASH_VERSION for the policy.
				// Missing `v` is treated as v: 1 — covers legacy URLs in chat/email/bookmarks
				// produced before the marker was introduced. An UNKNOWN v (greater than what
				// this client understands) means the URL was minted by a newer client; we
				// refuse to parse it rather than partial-restore from a shape we don't grok.
				const hashVersion = currentHashObject.v ?? this.URL_HASH_VERSION;
				if (hashVersion > this.URL_HASH_VERSION) {
					console.warn(
						"URL hash carries v=" +
							hashVersion +
							" but this client only understands v=" +
							this.URL_HASH_VERSION +
							"; leaving queryModel at defaults. The link was probably produced by a newer Pivotable build.",
					);
					return;
				}

				const queryModelFromHash = currentHashObject.query;

				this.parsedJsonToQueryModel(queryModelFromHash, queryModel);
			} catch (error) {
				// log but not re-throw as we do not want the hash to prevent the application from loading
				console.warn("Issue parsing queryModel from hash", currentHashDecoded, error);
			}
		}
	},

	queryModelToHash: function (currentHashDecoded, queryModel) {
		var currentHashObject;
		if (currentHashDecoded && currentHashDecoded.startsWith("#")) {
			currentHashObject = JSON.parse(currentHashDecoded.substring(1));
		} else {
			currentHashObject = {};
		}
		// Stamp the version on every emit — keeps the hash self-describing even when the
		// inbound hash was a legacy/unversioned one. See URL_HASH_VERSION for the policy.
		currentHashObject.v = this.URL_HASH_VERSION;
		currentHashObject.query = this.queryModelToParsedJson(queryModel);

		console.debug("Saving queryModel to hash", JSON.stringify(queryModel));

		return "#" + encodeURIComponent(JSON.stringify(currentHashObject));
	},

	queryModelToParsedJson: function (queryModel) {
		const parsedJson = {};

		parsedJson.columns = queryModel.columns();
		parsedJson.withStarColumns = queryModel.withStarColumns || {};
		parsedJson.measures = queryModel.measures();
		parsedJson.filter = queryModel.filter || {};
		parsedJson.customMarkers = queryModel.customMarkers || {};
		parsedJson.options = queryModel.options();

		return parsedJson;
	},

	parsedJsonToQueryModel: function (parsedJson, queryModel) {
		if (parsedJson) {
			// Reset first so this acts as a full snapshot REPLACEMENT — required for
			// browser back/forward to restore the exact prior view (otherwise stale
			// columns/measures/options from the current state would stick).
			queryModel.reset();

			for (const [columnIndex, columnName] of Object.entries(parsedJson.columns)) {
				queryModel.selectedColumns[columnName] = true;
				queryModel.onColumnToggled(columnName);
			}
			// In-place copy for the same reactivity reason that drove reset() to in-place
			// deletes: replacing the parent object breaks any downstream v-model that already
			// resolved to the previous proxy.
			Object.assign(queryModel.withStarColumns, parsedJson.withStarColumns || {});

			for (const [measureIndex, measureName] of Object.entries(parsedJson.measures)) {
				queryModel.selectedMeasures[measureName] = true;
			}
			Object.assign(queryModel.filter, parsedJson.filter || {});
			Object.assign(queryModel.customMarkers, parsedJson.customMarkers || {});

			for (const optionName of Object.values(parsedJson.options)) {
				queryModel.selectedOptions[optionName] = true;
			}

			console.debug("queryModel after loading from hash: ", JSON.stringify(queryModel));
		}
	},
};
