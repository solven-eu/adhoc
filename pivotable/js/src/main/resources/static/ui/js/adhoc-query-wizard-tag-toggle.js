// @ts-check

/**
 * Adds or removes a tag from the wizard's selected tags.
 *
 * The array is mutated in place rather than replaced: `searchOptions` is shared by reference across the wizard
 * components, so reassigning the property would detach the other holders from the update.
 *
 * @param {string[]} tags the currently selected tags
 * @param {string} tag the tag being toggled
 * @returns {boolean} true when the tag ends up selected, false when it ends up deselected
 */
export const toggleTag = function (tags, tag) {
	const tagIndex = tags.indexOf(tag);

	if (tagIndex >= 0) {
		tags.splice(tagIndex, 1);
		return false;
	}

	tags.push(tag);
	return true;
};
