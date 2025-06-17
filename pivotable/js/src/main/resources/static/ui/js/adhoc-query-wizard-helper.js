// Ordering of columns
import _ from "lodashEs";

export default {
	removeTag: function (searchOptions, tag) {
		const tags = searchOptions.tags;
		if (tags.includes(tag)) {
			// https://stackoverflow.com/questions/5767325/how-can-i-remove-a-specific-item-from-an-array-in-javascript
			const tagIndex = tags.indexOf(tag);
			tags.splice(tagIndex, 1);
		}
	},

	clearFilters: function (searchOptions) {
		searchOptions.text = "";
		// https://stackoverflow.com/questions/1232040/how-do-i-empty-an-array-in-javascript
		searchOptions.tags.length = 0;
	},

	queried: function (keyToBoolean) {
		return Object.entries(keyToBoolean)
			.filter((e) => e[1])
			.map((e) => e[0]);
	},

	filtered: function (searchOptions, inputsAsObjectOrArray) {
		const filtereditems = [];

		const searchedValue = searchOptions.text;
		const searchedValueLowerCase = searchedValue.toLowerCase();

		for (const inputKey in inputsAsObjectOrArray) {
			let matchAllTags = true;
			const inputElement = inputsAsObjectOrArray[inputKey];

			if (typeof inputElement === "boolean") {
				// case of options: accept if the option is active
				matchAllTags = inputElement;
			} else {
				if (typeof inputElement === "object" && searchOptions.tags.length >= 1) {
					// Accept only if input has at least one requested tag
					if (inputElement.tags) {
						for (const tag of searchOptions.tags) {
							if (!inputElement.tags.includes(tag)) {
								matchAllTags = false;
							}
						}
					} else {
						// No a single tag or input not taggable
					}
				}
			}

			let matchText = false;

			if (matchAllTags) {
				// We consider only values, as keys are generic
				// For instance, `name` should not match `name=NiceNick`
				const inputElementAsString = searchOptions.throughJson ? JSON.stringify(Object.values(inputElement)) : "";

				if (inputKey.includes(searchedValue) || inputElementAsString.includes(searchedValue)) {
					matchText = true;
				}

				if (!matchText && !searchOptions.caseSensitive) {
					// Retry without case-sensitivity
					if (inputKey.toLowerCase().includes(searchedValueLowerCase) || inputElementAsString.toLowerCase().includes(searchedValueLowerCase)) {
						matchText = true;
					}
				}
			} else {
				// not matching tags: no need to compute if matching text
			}

			if (matchAllTags && matchText) {
				if (Array.isArray(inputsAsObjectOrArray)) {
					filtereditems.push(inputElement);
				} else {
					// inputElement may be an Object or a primitive or a String
					if (typeof inputElement === "object") {
						filtereditems.push({ ...inputElement, ...{ key: inputKey } });
					} else {
						filtereditems.push({ key: inputKey, value: inputElement });
					}
				}
			}
		}

		// Measures has to be sorted by name
		// https://stackoverflow.com/questions/8996963/how-to-perform-case-insensitive-sorting-array-of-string-in-javascript
		return _.sortBy(filtereditems, [(resultItem) => (resultItem.key || resultItem.name).toLowerCase()]);
	},
};
