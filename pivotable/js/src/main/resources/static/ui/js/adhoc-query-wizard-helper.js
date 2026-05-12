// Ordering of columns. Per-function import to avoid fetching the lodash root bundle.
import sortBy from "lodashEs/sortBy.js";

// Waterfall match-rule tiers, highest-quality first. Each tier carries a score so that the wizard can show
// a percentage badge ("ccy" matches "Currency" at 50%, "Total" matches "total" at 90%, etc.), and the result
// list is ranked by descending score then alphabetically within each tier. Tier names mirror the spec in the
// chat-design conversation: exact > equals-ignore-case > contains > contains-i > subsequence > subsequence-i.
const MATCH_TIERS = [
	{ name: "exact", score: 100, caseSensitive: true },
	{ name: "equals-ignore-case", score: 90, caseSensitive: false },
	{ name: "contains", score: 80, caseSensitive: true },
	{ name: "contains-ignore-case", score: 70, caseSensitive: false },
	{ name: "subsequence", score: 60, caseSensitive: true },
	{ name: "subsequence-ignore-case", score: 50, caseSensitive: false },
];

// True when every character of `needle` appears in `haystack` in order (not necessarily contiguous).
// Powers the "subsequence" tiers — `ccy` → `Currency` succeeds because c, c, y all appear in order.
const isSubsequence = function (haystack, needle) {
	if (!needle) {
		return true;
	}
	let i = 0;
	for (let h = 0; h < haystack.length && i < needle.length; h++) {
		if (haystack[h] === needle[i]) {
			i++;
		}
	}
	return i === needle.length;
};

// Score a single haystack/needle pair against the waterfall. Returns 0 when no tier matches.
// When `caseSensitiveOnly` is true, skip the case-insensitive tiers entirely so the user's
// "Aa" toggle is honoured at every level of the waterfall, not just on the top-tier comparison.
const scoreSingle = function (haystack, needle, caseSensitiveOnly) {
	if (!haystack) {
		return 0;
	}
	if (!needle) {
		// Empty search matches everything with the top tier — keeps sort stable + alpha.
		return 100;
	}
	const haystackLc = haystack.toLowerCase();
	const needleLc = needle.toLowerCase();

	// Equals tiers — short-circuit on full-string match (most common case for a deliberate user search).
	if (haystack === needle) return 100;
	if (!caseSensitiveOnly && haystackLc === needleLc) return 90;

	// Contains tiers.
	if (haystack.includes(needle)) return 80;
	if (!caseSensitiveOnly && haystackLc.includes(needleLc)) return 70;

	// Subsequence tiers — power-tool match for compact abbreviations ("ccy" → "Currency").
	if (isSubsequence(haystack, needle)) return 60;
	if (!caseSensitiveOnly && isSubsequence(haystackLc, needleLc)) return 50;

	return 0;
};

// Compose the best score across multiple candidate haystacks (e.g. the input key + its JSON body when the
// "JSON" toggle is on). Returns the maximum because any one strong match should rank the item highly.
const scoreItem = function (haystacks, needle, caseSensitiveOnly) {
	let best = 0;
	for (const haystack of haystacks) {
		const s = scoreSingle(haystack, needle, caseSensitiveOnly);
		if (s > best) {
			best = s;
		}
		if (best === 100) {
			break;
		}
	}
	return best;
};

export default {
	// Exposed for unit tests — the waterfall is the single most testable piece of the wizard search.
	scoreSingle,
	scoreItem,
	isSubsequence,
	MATCH_TIERS,

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
		// Also drop the "show only queried entries" toggle. The wizard surfaces a Clear
		// button when the current filters yield zero matches; if that toggle was on, the
		// clear+text reset alone could still leave the user staring at an empty list
		// because `filtered()` would keep restricting to selected entries. Resetting it to
		// false ensures the user really sees ALL options after a single click.
		searchOptions.filterQueried = false;
	},

	queried: function (keyToBoolean) {
		return Object.entries(keyToBoolean)
			.filter((e) => e[1])
			.map((e) => e[0]);
	},

	// Discount applied to scores when an item matches text but does NOT satisfy the tag filter (the
	// drop-tags fallback below). Tunable knob — exported so tests can pin it.
	TAG_FALLBACK_DISCOUNT: 30,

	filtered: function (searchOptions, inputsAsObjectOrArray, queryModel) {
		const searchedValue = searchOptions.text || "";
		const caseSensitiveOnly = !!searchOptions.caseSensitive;
		const hasTags = Array.isArray(searchOptions.tags) && searchOptions.tags.length >= 1;
		const hasSearch = searchedValue.length > 0;

		// Score one item, optionally ignoring the tag filter. Returns 0 when the item is excluded.
		// `ignoreTags` is set on the second pass (tag-fallback) so an item that matched the search text but
		// failed the tag check still surfaces — at a discounted score.
		const scoreOneItem = (inputKey, inputElement, ignoreTags) => {
			if (searchOptions.filterQueried && queryModel) {
				if (JSON.stringify(queryModel).indexOf(inputKey) < 0) {
					return 0;
				}
			}

			if (typeof inputElement === "boolean") {
				return inputElement ? scoreItem([inputKey], searchedValue, caseSensitiveOnly) : 0;
			}

			if (!ignoreTags && hasTags && typeof inputElement === "object") {
				if (!inputElement.tags) {
					return 0;
				}
				for (const tag of searchOptions.tags) {
					if (!inputElement.tags.includes(tag)) {
						return 0;
					}
				}
			}

			const haystacks = [inputKey];
			if (searchOptions.throughJson && typeof inputElement === "object") {
				haystacks.push(JSON.stringify(Object.values(inputElement)));
			}
			return scoreItem(haystacks, searchedValue, caseSensitiveOnly);
		};

		const collect = (ignoreTags, scoreDiscount) => {
			const items = [];
			for (const inputKey in inputsAsObjectOrArray) {
				const inputElement = inputsAsObjectOrArray[inputKey];
				const raw = scoreOneItem(inputKey, inputElement, ignoreTags);
				if (raw <= 0) {
					continue;
				}
				const score = Math.max(1, raw - scoreDiscount);
				let item;
				if (Array.isArray(inputsAsObjectOrArray)) {
					item = inputElement;
				} else if (typeof inputElement === "object") {
					item = { ...inputElement, key: inputKey };
				} else {
					item = { key: inputKey, value: inputElement };
				}
				if (typeof item === "object" && item !== null) {
					// Stamp the score so the wizard can render a percentage badge per row.
					item._matchScore = score;
					// Flag rows that only made it into the result because we relaxed the tag filter, so the
					// wizard can render a visual chip telling the user "this row does NOT satisfy your
					// active tag selection". Without this hint the discounted score alone reads as ambiguous.
					if (ignoreTags && hasTags) {
						item._matchTagsBypassed = true;
					}
				}
				items.push(item);
			}
			return items;
		};

		const primary = collect(false, 0);

		// Drop-tags fallback: when the strict pass produced nothing AND the user had tags applied AND there
		// is search text, run a second pass that IGNORES the tag filter. Score those at a 30-point discount
		// so they read as "weaker matches" and the user has a visual cue that tags were dropped to get any
		// result at all. Without search text the user is just browsing — no fallback to surface, the empty
		// list is the right answer (their tag filter is too narrow).
		let fallback = [];
		if (primary.length === 0 && hasTags && hasSearch) {
			fallback = collect(true, this.TAG_FALLBACK_DISCOUNT);
		}

		const all = primary.concat(fallback);

		// Sort by score descending (so tier-1 hits come before tier-2, etc.), then alphabetically inside the
		// tier so equal-score rows stay deterministic. The previous behaviour (alpha only) is preserved when
		// no search text is set — every row scores 100 and ties break alphabetically.
		return sortBy(all, [(resultItem) => -(resultItem._matchScore || 100), (resultItem) => (resultItem.key || resultItem.name || "").toLowerCase()]);
	},
};
