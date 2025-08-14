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
		};

		queryModel.reset = function () {
			queryModel.selectedColumns = {};
			queryModel.selectedColumnsOrdered = [];
			// TODO withStarColumns may not be reset as they as some sort of preference
			// Still, they are resetted i nthis methods as a way to ensure the model is not corrupted
			queryModel.withStarColumns = {};
			queryModel.selectedMeasures = {};
			queryModel.filter = {};
			queryModel.customMarkers = {};
			queryModel.selectedOptions = {};
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

	hashToQueryModel: function (currentHashDecoded, queryModel) {
		// Restore queryModel from URL hash
		if (currentHashDecoded && currentHashDecoded.startsWith("#")) {
			try {
				const currentHashObject = JSON.parse(currentHashDecoded.substring(1));
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
			for (const [columnIndex, columnName] of Object.entries(parsedJson.columns)) {
				queryModel.selectedColumns[columnName] = true;
				queryModel.onColumnToggled(columnName);
			}
			queryModel.withStarColumns = parsedJson.withStarColumns || {};

			for (const [measureIndex, measureName] of Object.entries(parsedJson.measures)) {
				queryModel.selectedMeasures[measureName] = true;
			}
			queryModel.filter = parsedJson.filter || {};
			queryModel.customMarkers = parsedJson.customMarkers || {};

			for (const optionName of Object.values(parsedJson.options)) {
				queryModel.selectedOptions[optionName] = true;
			}

			console.debug("queryModel after loading from hash: ", JSON.stringify(queryModel));
		}
	},
};
