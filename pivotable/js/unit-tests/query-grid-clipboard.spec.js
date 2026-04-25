import { expect, test, vi } from "vitest";

import { escapeHtml, headerNameWithCopyIcon, extractCopyTarget, registerCopyNameDelegation } from "@/js/adhoc-query-grid-clipboard.js";

// Tiny DOM stub used by `extractCopyTarget`. Real Element nodes implement `closest`
// natively; here we hand-roll the minimum needed so the test runs in plain Node /
// Vitest without pulling jsdom into the dependency tree.
const fakeIcon = function (name) {
	const node = {
		className: "bi bi-clipboard adhoc-copy-name-btn",
		_name: name,
		getAttribute(attr) {
			return attr === "data-adhoc-name" ? this._name : null;
		},
		closest(selector) {
			if (selector === ".adhoc-copy-name-btn") return this;
			return null;
		},
	};
	return node;
};

const fakeNonIcon = function () {
	return {
		closest(selector) {
			return null;
		},
		getAttribute() {
			return null;
		},
	};
};

test("escapeHtml: escapes the OWASP-canon set", () => {
	expect(escapeHtml('<script>alert("&\'")</script>')).toBe("&lt;script&gt;alert(&quot;&amp;&#39;&quot;)&lt;/script&gt;");
});

test("headerNameWithCopyIcon: embeds the escaped name and the data attribute", () => {
	const html = headerNameWithCopyIcon('A "tricky" <name>');
	expect(html).toContain('<span class="adhoc-header-name">A &quot;tricky&quot; &lt;name&gt;</span>');
	expect(html).toContain('data-adhoc-name="A &quot;tricky&quot; &lt;name&gt;"');
	expect(html).toContain("adhoc-copy-name-btn");
});

test("extractCopyTarget: returns the column name when the click is on the icon", () => {
	expect(extractCopyTarget(fakeIcon("Country"))).toBe("Country");
});

test("extractCopyTarget: returns null when the click is outside any icon", () => {
	expect(extractCopyTarget(fakeNonIcon())).toBeNull();
	expect(extractCopyTarget(null)).toBeNull();
	expect(extractCopyTarget(undefined)).toBeNull();
});

// registerCopyNameDelegation regression test — the bug we are guarding against:
// clicking the copy icon ALSO triggered SlickGrid's column-sort handler. Root cause is
// that the click event bubbled up to `.slick-header-column` (where SlickGrid's sort
// listener lives) before reaching the grid container. The fix uses CAPTURE phase, so
// the container listener fires BEFORE the event descends to the column header — and
// `stopPropagation` actually stops the propagation that would otherwise reach the sort.
//
// To exercise the propagation contract without jsdom, we build a tiny event-listener
// shim on a plain object: `addEventListener` records (type, capture) and the test
// drives a fake event whose `stopPropagation` flips a flag.
test("registerCopyNameDelegation: subscribes click + mousedown in CAPTURE phase", () => {
	const calls = [];
	const containerEl = {
		addEventListener(type, fn, capture) {
			calls.push({ type, capture, fn });
		},
	};
	registerCopyNameDelegation(containerEl, () => {});
	expect(containerEl.__adhocCopyNameWired).toBe(true);
	const click = calls.find((c) => c.type === "click");
	const mouseDown = calls.find((c) => c.type === "mousedown");
	expect(click).toBeDefined();
	expect(mouseDown).toBeDefined();
	// Capture phase: the third argument is `true`. Without it, SlickGrid's bubble-phase
	// sort handler runs before our stopPropagation has a chance to fire.
	expect(click.capture).toBe(true);
	expect(mouseDown.capture).toBe(true);
});

test("registerCopyNameDelegation: clicking the icon stops propagation and fires onCopy", () => {
	const calls = [];
	const containerEl = {
		addEventListener(type, fn, capture) {
			calls.push({ type, fn });
		},
	};
	const onCopy = vi.fn();
	registerCopyNameDelegation(containerEl, onCopy);
	const clickHandler = calls.find((c) => c.type === "click").fn;
	const stopProp = vi.fn();
	const stopImmediate = vi.fn();
	const preventDefault = vi.fn();
	clickHandler({
		target: fakeIcon("Country"),
		stopPropagation: stopProp,
		stopImmediatePropagation: stopImmediate,
		preventDefault,
	});
	expect(onCopy).toHaveBeenCalledWith("Country");
	expect(stopProp).toHaveBeenCalled();
	expect(stopImmediate).toHaveBeenCalled();
	expect(preventDefault).toHaveBeenCalled();
});

test("registerCopyNameDelegation: clicking outside any icon is a no-op", () => {
	const calls = [];
	const containerEl = {
		addEventListener(type, fn) {
			calls.push({ type, fn });
		},
	};
	const onCopy = vi.fn();
	registerCopyNameDelegation(containerEl, onCopy);
	const clickHandler = calls.find((c) => c.type === "click").fn;
	const stopProp = vi.fn();
	clickHandler({ target: fakeNonIcon(), stopPropagation: stopProp, preventDefault: vi.fn() });
	expect(onCopy).not.toHaveBeenCalled();
	expect(stopProp).not.toHaveBeenCalled();
});

test("registerCopyNameDelegation: idempotent — second call does not double-wire", () => {
	let listenerCount = 0;
	const containerEl = {
		addEventListener() {
			listenerCount++;
		},
	};
	registerCopyNameDelegation(containerEl, () => {});
	const after1 = listenerCount;
	registerCopyNameDelegation(containerEl, () => {});
	expect(listenerCount).toBe(after1);
});
