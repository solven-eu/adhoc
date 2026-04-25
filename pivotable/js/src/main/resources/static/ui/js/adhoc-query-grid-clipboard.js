// Pure helpers for the per-column-header copy-name affordance.
//
// Extracted from `adhoc-query-grid-helper.js` so they can be unit-tested without
// pulling in SlickGrid / Sortable / lodash / bootstrap (which the Vitest node
// environment cannot resolve). The helper file there re-exports
// `copyColumnNameToClipboard` and the markup builder; the runtime click delegation
// uses `extractCopyTarget` to decide whether a click event is for us.

// HTML-escape a column / measure name before splicing it into the header markup. The
// name comes from the server schema and is normally a plain identifier, but defending
// against an attacker-controlled cube descriptor with a script payload in a column
// name costs nothing.
export const escapeHtml = function (s) {
	return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
};

// Build the HTML used as `column.name` in SlickGrid: the bare name followed by a small
// clipboard icon. SlickGrid renders `column.name` as innerHTML, so the icon ends up
// immediately next to the name itself rather than off to the right (where the
// `column.header.buttons` plugin places its icons). The click handler is wired by event
// delegation in `registerHeaderButtons` — we cannot attach a listener here because
// SlickGrid recreates the header DOM on every `setColumns()`.
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

// Given a click target node, return the column name to copy IF the click was on the
// inline copy-name icon (or one of its descendants), otherwise null.
//
// Pure function with no side effects — the unit test can call this with a jsdom-free
// mock element and assert the right name is returned. The caller is responsible for
// stopping event propagation and for triggering the actual clipboard write.
export const extractCopyTarget = function (targetEl) {
	if (!targetEl || typeof targetEl.closest !== "function") return null;
	const btn = targetEl.closest(".adhoc-copy-name-btn");
	if (!btn) return null;
	return btn.getAttribute("data-adhoc-name") || "";
};

// Register the click-delegation listener on the SlickGrid container. Crucial detail:
// the listener uses CAPTURE phase. Without that, the event reaches
// `.slick-header-column` (where SlickGrid attaches its sort handler) BEFORE bubbling up
// to the container — by which point our `stopPropagation` is too late and the column
// re-orders alongside the copy. With capture, our listener fires before the event
// descends to the column header, so `stopPropagation` actually works.
//
// The `__adhocCopyNameWired` guard prevents double-registration if the grid is rebuilt.
export const registerCopyNameDelegation = function (containerEl, onCopy) {
	if (!containerEl || containerEl.__adhocCopyNameWired) return;
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
