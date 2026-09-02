// @ts-check

/**
 * Narrows the measure selection down to a single measure — the "Keep only" action familiar from pivot tools, and the
 * complement of removing a measure one at a time when a query carries many of them.
 *
 * Only currently-selected measures are cleared, so the operation can only ever narrow what the grid shows: a measure
 * the user had not picked is not silently introduced.
 *
 * The map is mutated in place rather than replaced, because `queryModel.selectedMeasures` is held by reference across
 * the wizard, the grid and the query executor.
 *
 * @param {Record<string, boolean>} selectedMeasures measure name to whether it is currently selected
 * @param {string} measureName the measure to keep
 * @returns {string[]} the measures which were deselected, in iteration order
 */
export const keepOnlyMeasure = function (selectedMeasures, measureName) {
	const deselected = [];

	for (const candidate of Object.keys(selectedMeasures)) {
		if (candidate !== measureName && selectedMeasures[candidate]) {
			selectedMeasures[candidate] = false;
			deselected.push(candidate);
		}
	}

	// The kept measure is normally already selected — it is being acted on from its own grid column. Setting it
	// explicitly keeps the post-condition true whatever the caller passed.
	selectedMeasures[measureName] = true;

	return deselected;
};
