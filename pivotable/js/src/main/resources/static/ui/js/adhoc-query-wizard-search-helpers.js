/**
 * Helps marking elements macthing a Wizard search
 */
export function markMatchingWizard(searchOptions, text) {
	if (!searchOptions.text) {
		// No regex: nothing to highlight/mark
		return text;
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
	function escapeRegex(string) {
		return string.replace(/[/\-\\^$*+?.()|[\]{}]/g, "\\$&");
	}
	const quotedText = escapeRegex(searchOptions.text);

	// https://bitsofco.de/a-one-line-solution-to-highlighting-search-matches/
	return text.replace(new RegExp(quotedText, flags), (match) => `<mark>${match}</mark>`);
}
