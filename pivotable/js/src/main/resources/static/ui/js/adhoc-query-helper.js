
import wizardHelper from "./adhoc-query-wizard-helper.js";

export default {
	makeQueryModel: function() {
			const queryModel = 		{
					// `columnName->boolean`
					selectedColumns: {},
					// `columnName->boolean`
					withStarColumns: {},
					// `measureName->boolean`
					selectedMeasures: {},
					// `orderedArray of columnNames`
					selectedColumnsOrdered: [],
					customMarkers: {},
					// `optionName->boolean`
					selectedOptions: {},
					};
					

					queryModel.reset= function () {
						queryModel.selectedMeasures = {};
						queryModel.selectedColumns = {};
						queryModel.selectedColumnsOrdered = [];
						// TODO withStarColumns may not be reset as they as some sort of preference
						// Still, they are resetted i nthis methods as a way to ensure the model is not corrupted 
						queryModel.withStarColumns = {};
						queryModel.selectedOptions = {};
						queryModel.customMarkers = {};
						console.log("queryModel has been reset");
					};

					queryModel.onColumnToggled= function (column) {
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
					}
		
					return queryModel;


	},
	
	hashToQueryModel: function (currentHashDecoded, queryModel) {
		// Restore queryModel from URL hash
		if (currentHashDecoded && currentHashDecoded.startsWith("#")) {
			try {
				const currentHashObject = JSON.parse(currentHashDecoded.substring(1));
				const queryModelFromHash = currentHashObject.query;

				if (queryModelFromHash) {
					for (const [columnIndex, columnName] of Object.entries(queryModelFromHash.columns)) {
						queryModel.selectedColumns[columnName] = true;
						queryModel.onColumnToggled(columnName);
					}
					queryModel.withStarColumns = queryModelFromHash.withStarColumns || {};
					
					for (const [measureIndex, measureName] of Object.entries(queryModelFromHash.measures)) {
						queryModel.selectedMeasures[measureName] = true;
					}
					queryModel.filter = queryModelFromHash.filter || {};
					queryModel.customMarkers = queryModelFromHash.customMarkers || {};

					for (const optionName of Object.values(queryModelFromHash.options)) {
						queryModel.selectedOptions[optionName] = true;
					}

					console.debug("queryModel after loading from hash: ", JSON.stringify(queryModel));
				}
			} catch (error) {
				// log but not re-throw as we do not want the hash to prevent the application from loading
				console.warn("Issue parsing queryModel from hash", currentHashDecoded, error);
			}
		}
	},
	
	queryModelToHash: function(currentHashDecoded, queryModel) {
		var currentHashObject;
		if (currentHashDecoded && currentHashDecoded.startsWith("#")) {
			currentHashObject = JSON.parse(currentHashDecoded.substring(1));
		} else {
			currentHashObject = {};
		}
		currentHashObject.query = {};
		currentHashObject.query.columns = Object.values(queryModel.selectedColumnsOrdered || {});
		currentHashObject.query.withStarColumns = queryModel.withStarColumns || {};
		currentHashObject.query.measures = wizardHelper.queried(queryModel.selectedMeasures || {});
		currentHashObject.query.filter = queryModel.filter || {};
		currentHashObject.query.customMarkers = queryModel.customMarkers || {};
		currentHashObject.query.options = wizardHelper.queried(queryModel.selectedOptions || {});

		console.debug("Saving queryModel to hash", JSON.stringify(queryModel));

		return "#" + encodeURIComponent(JSON.stringify(currentHashObject));
	}
};
