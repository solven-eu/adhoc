// @ts-check

// Deterministic colour assignment for tag badges (cube tags, and eventually anywhere else we
// surface a tag string). The output is stable across reloads — the same tag always lights up
// the same hue — without requiring a curated palette to grow alongside the tag set.
//
// HSL is chosen over hex/RGB because saturation and lightness can be fixed at readable
// mid-tone values, leaving the hash to drive only the hue (0-360). That keeps every
// auto-coloured badge in the same legibility band — no accidentally near-white or near-black
// colours — and white text on top always contrasts well.
//
// `tagToStyle(tag, override)` accepts an optional explicit colour so a future
// backend-provided override (e.g. via the public config API the user mentioned) can plug in
// here without changing the call sites: the caller passes `tagToStyle(tag, configMap[tag])`
// and we honour the override when present, otherwise compute deterministically.

const SATURATION_PCT = 55;
const LIGHTNESS_PCT = 42;

/**
 * Hash a string into a 32-bit integer. Same algorithm as the project's existing
 * `String.prototype.hashCode` (FNV-style accumulation) — kept inline here so this helper has
 * no side-effect imports (the prototype extension lives in `adhoc-query-helper.js`).
 *
 * @param {string} input
 * @returns {number}
 */
function stableHash(input) {
	let hash = 0;
	const s = String(input || "");
	for (let i = 0; i < s.length; i++) {
		hash = (hash << 5) - hash + s.charCodeAt(i);
		hash |= 0; // force 32-bit
	}
	return hash;
}

/**
 * Pick a stable hue (0-360) from a tag string. Negative hashes wrap around via the modulo on
 * the absolute value so the output is always non-negative.
 *
 * @param {string} tag
 * @returns {number}
 */
export function tagToHue(tag) {
	return Math.abs(stableHash(tag)) % 360;
}

/**
 * Return an inline `style` string ready to drop on a badge element. The hue is auto-computed
 * from the tag when no override is supplied; passing an explicit colour (e.g. `#1f6feb`) wins.
 * The text colour is always white for readability against the mid-tone fill.
 *
 * @param {string} tag
 * @param {string} [override] explicit colour (any CSS colour value) — wins over the auto-computed one
 * @returns {string}
 */
export function tagToStyle(tag, override) {
	if (override) {
		return "background-color: " + override + "; color: white;";
	}
	const hue = tagToHue(tag);
	return "background-color: hsl(" + hue + ", " + SATURATION_PCT + "%, " + LIGHTNESS_PCT + "%); color: white;";
}
