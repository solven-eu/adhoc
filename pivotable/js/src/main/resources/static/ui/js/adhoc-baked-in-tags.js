// @ts-check

/**
 * Tags which carry meaning to Pivotable, as opposed to the free-form tags a cube author attaches for their own
 * classification.
 *
 * Most of these are stamped by the engine rather than chosen by a user — a reader of the tag list has no way to guess
 * what `composite-full` means, so each one is described here next to whatever behaviour it drives. Keep the two
 * together: a description that drifts from the behaviour is worse than none.
 */

/** Measures and columns worth starting from. Selected by default on a first visit. */
export const TAG_ESSENTIAL = "essential";

/** Measures and columns which should stay out of the way unless explicitly asked for. */
export const TAG_HIDDEN = "hidden";

/**
 * Descriptions for the tags Pivotable knows about. A cube may describe its own tags through the cube description API;
 * those take precedence, see {@link describeTag}.
 *
 * @type {Record<string, string>}
 */
export const BAKED_IN_TAG_DESCRIPTIONS = {
	[TAG_ESSENTIAL]: "Useful in most cases. Selected by default on your first visit, so a large cube opens on a workable subset.",
	[TAG_HIDDEN]: "Kept out of the wizard unless this tag is selected. Typically plumbing a cube author does not want surfaced.",
	calculated: "Column computed by the cube rather than read from the underlying table.",
	generated: "Column whose coordinates are produced by a column generator rather than stored in the table.",
	meta: "Column describing the cube itself, such as which sub-cube a row came from.",
	"composite-full": "Column present in every sub-cube of a composite cube, as opposed to only some of them.",
	technical: "Engine plumbing, carrying no business meaning.",
};

/**
 * The description to show for a tag, preferring what the cube says about its own tags over Pivotable's built-in
 * knowledge — a cube author naming a tag `meta` for their own purposes should win over our reading of it.
 *
 * @param {string} tag
 * @param {Record<string, string> | undefined | null} cubeTagDescriptions descriptions carried by the cube description
 * @returns {string} the description, or an empty string when nothing describes this tag
 */
export const describeTag = function (tag, cubeTagDescriptions) {
	if (cubeTagDescriptions && typeof cubeTagDescriptions[tag] === "string") {
		return cubeTagDescriptions[tag];
	}
	return BAKED_IN_TAG_DESCRIPTIONS[tag] || "";
};

/**
 * Whether an item (measure or column) carries the given tag. Tolerates items without tags, as an error column may not
 * carry any.
 *
 * @param {{tags?: string[]} | undefined | null} item
 * @param {string} tag
 * @returns {boolean}
 */
export const hasTag = function (item, tag) {
	return !!item && Array.isArray(item.tags) && item.tags.includes(tag);
};

/**
 * Whether an item must be kept out of the wizard listing.
 *
 * Selecting the `hidden` tag is the escape hatch: a user who explicitly asks for hidden items is asking to see them, so
 * the exclusion lifts rather than yielding a list that is empty for no visible reason.
 *
 * @param {{tags?: string[]} | undefined | null} item
 * @param {string[] | undefined | null} selectedTags the tags currently selected in the wizard
 * @returns {boolean}
 */
export const isExcludedAsHidden = function (item, selectedTags) {
	if (!hasTag(item, TAG_HIDDEN)) {
		return false;
	}
	return !(Array.isArray(selectedTags) && selectedTags.includes(TAG_HIDDEN));
};

/**
 * Every tag carried by a cube's measures and columns, deduplicated.
 *
 * @param {any} cube as held by the adhoc store
 * @returns {string[]}
 */
export const collectCubeTags = function (cube) {
	const tags = new Set();

	if (!cube) {
		return [];
	}

	for (const measure of Object.values(cube.measures || {})) {
		for (const tag of /** @type {any} */ (measure).tags || []) {
			tags.add(tag);
		}
	}
	// tags may be missing on an error column
	for (const column of Object.values(cube.columns?.columns || {})) {
		for (const tag of /** @type {any} */ (column).tags || []) {
			tags.add(tag);
		}
	}

	return Array.from(tags);
};

/**
 * The tags to select on a first visit, given what the cube offers.
 *
 * Returns nothing unless the cube actually carries {@link TAG_ESSENTIAL} — defaulting to a tag no item holds would
 * filter the wizard down to an empty list, which reads as a broken cube.
 *
 * @param {string[] | undefined | null} availableTags every tag carried by the cube's measures and columns
 * @returns {string[]} the tags to preselect
 */
export const defaultSelectedTags = function (availableTags) {
	if (Array.isArray(availableTags) && availableTags.includes(TAG_ESSENTIAL)) {
		return [TAG_ESSENTIAL];
	}
	return [];
};
