// @ts-check

/**
 * The measures and columns a query actually carries, as flat name lists.
 *
 * `selectedMeasures` / `selectedColumns` are name-to-boolean maps, where an entry set to false is a remembered but
 * unselected item — so the keys alone are not the answer.
 *
 * @param {{selectedMeasures?: Record<string, boolean>, selectedColumns?: Record<string, boolean>}} queryModel
 * @returns {{measures: string[], columns: string[]}} names in insertion order
 */
export const involvedInQuery = function (queryModel) {
	const selected = function (nameToSelected) {
		if (!nameToSelected) {
			return [];
		}
		return Object.keys(nameToSelected).filter((name) => nameToSelected[name]);
	};

	return {
		measures: selected(queryModel && queryModel.selectedMeasures),
		columns: selected(queryModel && queryModel.selectedColumns),
	};
};

/**
 * Shortest name considered for {@link mentionedInError}. A one or two character name matches almost any message by
 * accident, and a false highlight pointing at an innocent measure is worse than no highlight at all.
 */
export const MIN_MENTION_LENGTH = 3;

/**
 * Whether an error message appears to be talking about a given measure or column.
 *
 * This is a plain substring test, NOT an analysis of the error: it is a hint for the eye, not a diagnosis. A name that
 * happens to occur inside an unrelated word will match, so the caller must present the result as a suggestion rather
 * than as the cause.
 *
 * @param {string | undefined | null} errorMessage
 * @param {string} name a measure or column name
 * @returns {boolean}
 */
export const mentionedInError = function (errorMessage, name) {
	if (!errorMessage || !name || name.length < MIN_MENTION_LENGTH) {
		return false;
	}
	return errorMessage.includes(name);
};
