// @ts-check

// Human-readable rendering of a queryModel valueMatcher. The wizard's filter chip used to
// render the matcher object directly via `{{filter.valueMatcher}}`, which produced raw JSON
// like `{"type":"not","negated":"Darla K. Anderson"}` for negated equals — unreadable.
//
// This helper unwraps the common shapes (primitive, equals, not, not-equals) into a small
// `{text, strike, op}` shape the template can render cleanly:
//   - `text`   — the displayed value (operand on the right of the operator)
//   - `op`     — the operator glyph: "=" for inclusion, "≠" for exclusion
//   - `strike` — true when the value should be rendered with a strikethrough (i.e. the user
//                said "NOT this"). Pairs visually with the ≠ operator for redundant
//                emphasis — the strikethrough alone is enough to make a not-equals stand
//                out at a glance, the ≠ confirms the semantics.
//
// Unrecognised shapes fall back to `JSON.stringify` so the chip stays readable (and obviously
// labelled as raw JSON) rather than silently rendering `[object Object]`.

/**
 * @typedef {Object} RenderedMatcher
 * @property {string} text  the displayed value (the operand on the right of the operator)
 * @property {"="|"≠"} op   operator glyph
 * @property {boolean} strike true when the value should be rendered with a strikethrough
 */

/**
 * @param {any} valueMatcher
 * @returns {RenderedMatcher}
 */
export function renderValueMatcher(valueMatcher) {
	if (valueMatcher === null || valueMatcher === undefined) {
		return { text: "", op: "=", strike: false };
	}
	if (typeof valueMatcher !== "object") {
		// Primitive: render as-is. Most common shape — a raw "France" or 42.
		return { text: String(valueMatcher), op: "=", strike: false };
	}
	const t = valueMatcher.type;
	if (t === "equals" && valueMatcher.operand !== undefined) {
		return { text: stringifyOperand(valueMatcher.operand), op: "=", strike: false };
	}
	if (t === "not" && valueMatcher.negated !== undefined) {
		// Recurse one level so a `not(equals(x))` renders as the bare `x` with strikethrough +
		// ≠, just like `not(x)` would. We don't recurse further than that — a NOT-of-OR is
		// rare and the JSON.stringify fallback is acceptable for it.
		const inner = valueMatcher.negated;
		if (inner === null || inner === undefined) {
			return { text: "", op: "≠", strike: true };
		}
		if (typeof inner !== "object") {
			return { text: String(inner), op: "≠", strike: true };
		}
		if (inner.type === "equals" && inner.operand !== undefined) {
			return { text: stringifyOperand(inner.operand), op: "≠", strike: true };
		}
		return { text: JSON.stringify(inner), op: "≠", strike: true };
	}
	// Unknown matcher shape — preserve as JSON so the user can see what's going on rather
	// than silently rendering `[object Object]`.
	return { text: JSON.stringify(valueMatcher), op: "=", strike: false };
}

function stringifyOperand(operand) {
	if (operand === null || operand === undefined) return "";
	if (typeof operand === "object") return JSON.stringify(operand);
	return String(operand);
}
