// @ts-check
// Pure helpers for the per-column-header copy-name affordance.
//
// Extracted from `adhoc-query-grid-helper.js` so they can be unit-tested without
// pulling in SlickGrid / Sortable / lodash / bootstrap (which the Vitest node
// environment cannot resolve). The helper file there re-exports
// `copyColumnNameToClipboard` and the markup builder; the runtime click delegation
// uses `extractCopyTarget` to decide whether a click event is for us.

/**
 * HTML-escape a column / measure name before splicing it into the header markup. The name comes from the server
 * schema and is normally a plain identifier, but defending against an attacker-controlled cube descriptor with a
 * script payload in a column name costs nothing.
 *
 * @param {unknown} s the value to escape; coerced to a String first
 * @returns {string} an HTML-safe rendering of `s`
 */
export const escapeHtml = function (s) {
	/** @type {Record<string, string>} */
	const ENTITIES = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };
	return String(s).replace(/[&<>"']/g, (c) => ENTITIES[c]);
};

/**
 * Build the HTML used as `column.name` in SlickGrid: the bare name followed by a small clipboard icon. SlickGrid
 * renders `column.name` as innerHTML, so the icon ends up immediately next to the name itself rather than off to
 * the right (where the `column.header.buttons` plugin places its icons). The click handler is wired by event
 * delegation in `registerHeaderButtons` — we cannot attach a listener here because SlickGrid recreates the header
 * DOM on every `setColumns()`.
 *
 * @param {string} rawName the column name as it appears in the server schema
 * @returns {string} HTML markup to set on `column.name`
 */
export const headerNameWithCopyIcon = function (rawName) {
	const safe = escapeHtml(rawName);
	return (
		'<span class="adhoc-header-name">' +
		safe +
		"</span>" +
		' <i class="bi bi-clipboard adhoc-copy-name-btn" data-adhoc-name="' +
		safe +
		'" role="button" tabindex="0" title="Copy name to clipboard" style="cursor: pointer; opacity: 0.5; margin-left: 0.25rem"></i>'
	);
};

/**
 * Given a click target node, return the column name to copy IF the click was on the inline copy-name icon (or one
 * of its descendants), otherwise null.
 *
 * <p>Pure function with no side effects — the unit test can call this with a jsdom-free mock element and assert
 * the right name is returned. The caller is responsible for stopping event propagation and for triggering the
 * actual clipboard write.
 *
 * @param {({ closest: (selector: string) => ({ getAttribute: (attr: string) => string | null } | null) }) | null | undefined} targetEl the event target (typically `event.target`); typed structurally so the unit tests can pass a stub
 * @returns {string | null} the copy target name, or {@code null} when the click is not on the copy icon
 */
export const extractCopyTarget = function (targetEl) {
	if (!targetEl || typeof targetEl.closest !== "function") {
		return null;
	}
	const btn = targetEl.closest(".adhoc-copy-name-btn");
	if (!btn) {
		return null;
	}
	return btn.getAttribute("data-adhoc-name") || "";
};

/**
 * Register the click-delegation listener on the SlickGrid container. Crucial detail: the listener uses CAPTURE
 * phase. Without that, the event reaches `.slick-header-column` (where SlickGrid attaches its sort handler) BEFORE
 * bubbling up to the container — by which point our `stopPropagation` is too late and the column re-orders
 * alongside the copy. With capture, our listener fires before the event descends to the column header, so
 * `stopPropagation` actually works.
 *
 * <p>The `__adhocCopyNameWired` guard prevents double-registration if the grid is rebuilt.
 *
 * @param {({ addEventListener: (type: string, listener: (e: any) => any, capture?: any) => any, __adhocCopyNameWired?: boolean }) | null | undefined} containerEl the grid container node (typed structurally so the unit tests can pass a stub)
 * @param {(name: string) => void} onCopy callback fired with the resolved name when the icon is clicked
 */
export const registerCopyNameDelegation = function (containerEl, onCopy) {
	if (!containerEl || containerEl.__adhocCopyNameWired) {
		return;
	}
	containerEl.__adhocCopyNameWired = true;
	containerEl.addEventListener(
		"click",
		function (e) {
			const name = extractCopyTarget(e.target);
			if (name === null) return;
			// Capture-phase + stopPropagation so SlickGrid's sort handler on the
			// header-column element NEVER fires for this click.
			e.preventDefault();
			e.stopPropagation();
			if (typeof e.stopImmediatePropagation === "function") {
				e.stopImmediatePropagation();
			}
			if (typeof onCopy === "function") onCopy(name);
		},
		true,
	);
	// Some browsers route `mousedown`-driven sort interactions before `click` ever
	// fires. Stop those too on the icon so a quick down→up sequence on the icon
	// cannot trigger a sort even on browsers that initiate the sort on mousedown.
	containerEl.addEventListener(
		"mousedown",
		function (e) {
			const name = extractCopyTarget(e.target);
			if (name === null) return;
			e.stopPropagation();
			if (typeof e.stopImmediatePropagation === "function") {
				e.stopImmediatePropagation();
			}
		},
		true,
	);
};
