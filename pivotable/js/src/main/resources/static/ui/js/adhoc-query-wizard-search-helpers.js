// @ts-check

/**
 * @typedef WizardSearchOptions
 * @property {string} [text] regex-free string the user is searching for; empty / undefined → no highlighting
 * @property {boolean} [caseSensitive] when true, the match preserves case
 */

/**
 * Wraps occurrences of {@code searchOptions.text} inside {@code text} with {@code <mark>...</mark>} so the rendered
 * HTML highlights every match.
 *
 * @param {WizardSearchOptions} searchOptions the search state from the Wizard panel
 * @param {unknown} text the value to highlight; non-strings are JSON-stringified first (Shiftor-style objects)
 * @returns {string} the input text with `<mark>` wrappers around each match, or unchanged when no search is active
 */
export function markMatchingWizard(searchOptions, text) {
	/** @type {string} */
	let str;
	if (typeof text === "string") {
		str = text;
	} else {
		// BEWARE This happens on `Shiftor`
		// TODO We should mark only along values, not keys
		str = JSON.stringify(text);
	}

	if (!searchOptions.text) {
		// No regex: nothing to highlight/mark
		return str;
	}

	// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/RegExp/global
	// `global` as we want to highlight all matches
	var flags = "g";

	// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/RegExp/ignoreCase
	if (!searchOptions.caseSensitive) {
		flags += "i";
	}

	// `searchOptions.text` has to be matched as text, not interpreted as a regex expression
	// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/RegExp/escape
	// `RegExp.escape` is not in Chrome

	// https://stackoverflow.com/questions/3561493/is-there-a-regexp-escape-function-in-javascript
	/**
	 * @param {string} s the literal text to escape so it can be embedded inside a new RegExp
	 * @returns {string} the escaped form, where every regex-special character is back-slashed
	 */
	function escapeRegex(s) {
		return s.replace(/[/\-\\^$*+?.()|[\]{}]/g, "\\$&");
	}
	const quotedText = escapeRegex(searchOptions.text);

	// https://bitsofco.de/a-one-line-solution-to-highlighting-search-matches/
	return str.replace(new RegExp(quotedText, flags), (match) => `<mark>${match}</mark>`);
}
