// @ts-check
// Pure helpers for the "Ctrl+C copies the clicked cell" UX on the grid. Extracted from the helper
// module so the keyboard matcher + the cell-text projection can be vitest-tested without a DOM or a
// SlickGrid instance.
//
// The mounted side (subscribing to `grid.onClick` + `grid.onKeyDown`) lives in
// `adhoc-query-grid-helper.js` — it's a thin DOM glue layer on top of the helpers here.

/**
 * @typedef {{ ctrlKey?: boolean, metaKey?: boolean, shiftKey?: boolean, altKey?: boolean, key?: string, target?: any }} KeyEventLike
 */

/**
 * Does the event match the OS-level "copy" shortcut (Ctrl+C on Windows/Linux, ⌘+C on macOS)?
 *
 * <p>Returns {@code false} when:
 * <ul>
 *   <li>The target is an editable element (input/textarea/contenteditable). Pressing Ctrl+C in a
 *       text field MUST defer to the browser's built-in selection-copy — otherwise the user
 *       can't copy a partial substring out of, say, the filter input.</li>
 *   <li>Modifier mix is wrong: Shift+Ctrl+C / Alt+Ctrl+C are reserved for other features.</li>
 *   <li>Both Ctrl AND Cmd are held — typically a stuck modifier; safer to bail.</li>
 * </ul>
 *
 * @param {KeyEventLike} event the event-like object (real `KeyboardEvent` or a test stub)
 * @returns {boolean}
 */
export function isCopyShortcut(event) {
	if (!event) return false;
	if (event.key !== "c" && event.key !== "C") return false;
	const ctrl = event.ctrlKey === true;
	const meta = event.metaKey === true;
	// Exactly one of Ctrl / Cmd must be held — XOR. Both held together looks like a stuck modifier.
	if (ctrl === meta) return false;
	if (event.shiftKey || event.altKey) return false;
	// Defer to the browser's built-in text-selection copy inside editable controls. Without this
	// guard, typing into the filter input and pressing Ctrl+C would copy a stale grid cell instead
	// of the user's selected substring.
	if (isEditableTarget(event.target)) return false;
	return true;
}

/**
 * Is the event's {@code target} an element where the browser owns Ctrl+C semantics (text input,
 * textarea, contenteditable)? Returns {@code false} for grid cells / canvas / div containers.
 *
 * @param {any} target
 * @returns {boolean}
 */
function isEditableTarget(target) {
	if (!target || typeof target !== "object") return false;
	const tag = typeof target.tagName === "string" ? target.tagName.toUpperCase() : "";
	if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return true;
	if (target.isContentEditable === true) return true;
	return false;
}

/**
 * Project a raw grid-cell value to the clipboard string the user expects to see pasted. Cells in
 * this grid carry whatever the dataView returned for the column id; common shapes:
 * <ul>
 *   <li>Primitive (number, string, boolean) → coerced via {@code String(value)}, with {@code null}
 *       and {@code undefined} returning the empty string.</li>
 *   <li>Formatter-returned HTML fragment ({@code "<span>EUR</span>"}) → tags stripped so the
 *       clipboard carries plain text matching what's visibly rendered in the cell.</li>
 *   <li>Object / array → {@code JSON.stringify} as a defensive fallback. In practice the grid
 *       doesn't render objects (they'd appear as {@code [object Object]} otherwise), so this is a
 *       safety net rather than a primary path.</li>
 * </ul>
 *
 * @param {unknown} value the raw cell value from the dataView item
 * @returns {string}
 */
export function extractCellText(value) {
	if (value == null) return "";
	if (typeof value === "string") {
		return stripHtmlTags(value);
	}
	if (typeof value === "number" || typeof value === "boolean") {
		return String(value);
	}
	// Object / array: JSON-encode as the least-surprising fallback. Wrapped in try/catch for the
	// degenerate cyclic-reference case — falls back to `String(...)` which yields "[object Object]"
	// but doesn't throw the keyboard handler.
	try {
		return JSON.stringify(value);
	} catch {
		return String(value);
	}
}

/**
 * Iteratively strip HTML tags from {@code text}. Same shape as the strip in
 * `adhoc-query-grid-autofit.js` (CodeQL-recommended re-run-until-stable to defeat nested-tag
 * payloads that would survive a single-pass regex).
 *
 * @param {string} text
 * @returns {string}
 */
function stripHtmlTags(text) {
	let previous;
	let current = text;
	do {
		previous = current;
		current = current.replace(/<[^>]+>/g, "");
	} while (current !== previous);
	return current;
}
