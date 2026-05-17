// @ts-check
// Pure tests for the Ctrl/Cmd+C copy-cell helpers. No DOM, no SlickGrid — both functions accept
// plain JS objects.

import { describe, expect, test } from "vitest";

import { extractCellText, isCopyShortcut } from "@/js/adhoc-query-grid-copy-cell.js";

describe("isCopyShortcut", () => {
	test("Ctrl+C matches", () => {
		expect(isCopyShortcut({ key: "c", ctrlKey: true })).toBe(true);
	});

	test("Cmd+C (metaKey) matches — macOS shortcut", () => {
		expect(isCopyShortcut({ key: "c", metaKey: true })).toBe(true);
	});

	test("uppercase C also matches (keyboard layout / shift quirks)", () => {
		expect(isCopyShortcut({ key: "C", ctrlKey: true })).toBe(true);
	});

	test("plain C without modifier does NOT match (just typing 'c')", () => {
		expect(isCopyShortcut({ key: "c" })).toBe(false);
	});

	test("Shift+Ctrl+C does NOT match — reserved for other shortcuts", () => {
		expect(isCopyShortcut({ key: "c", ctrlKey: true, shiftKey: true })).toBe(false);
	});

	test("Alt+Ctrl+C does NOT match", () => {
		expect(isCopyShortcut({ key: "c", ctrlKey: true, altKey: true })).toBe(false);
	});

	test("Ctrl+C inside an INPUT defers to the browser's text-copy", () => {
		expect(isCopyShortcut({ key: "c", ctrlKey: true, target: { tagName: "INPUT" } })).toBe(false);
	});

	test("Ctrl+C inside a TEXTAREA defers to the browser's text-copy", () => {
		expect(isCopyShortcut({ key: "c", ctrlKey: true, target: { tagName: "textarea" } })).toBe(false);
	});

	test("Ctrl+C inside a contentEditable defers to the browser's text-copy", () => {
		expect(isCopyShortcut({ key: "c", ctrlKey: true, target: { tagName: "DIV", isContentEditable: true } })).toBe(false);
	});

	test("Both Ctrl AND Cmd held → bail (stuck modifier defence)", () => {
		expect(isCopyShortcut({ key: "c", ctrlKey: true, metaKey: true })).toBe(false);
	});

	test("Non-c key with Ctrl held does NOT match (Ctrl+V, Ctrl+X, etc.)", () => {
		expect(isCopyShortcut({ key: "v", ctrlKey: true })).toBe(false);
		expect(isCopyShortcut({ key: "x", ctrlKey: true })).toBe(false);
	});

	test("null / undefined event does not throw", () => {
		// JSDoc types don't reject nullish here.
		expect(isCopyShortcut(/** @type {any} */ (null))).toBe(false);
		expect(isCopyShortcut(/** @type {any} */ (undefined))).toBe(false);
	});
});

describe("extractCellText", () => {
	test("string passes through unchanged", () => {
		expect(extractCellText("hello")).toBe("hello");
	});

	test("number coerced to its String representation", () => {
		expect(extractCellText(42)).toBe("42");
		expect(extractCellText(3.14)).toBe("3.14");
		expect(extractCellText(0)).toBe("0");
	});

	test("boolean coerced to its String representation", () => {
		expect(extractCellText(true)).toBe("true");
		expect(extractCellText(false)).toBe("false");
	});

	test("null / undefined produce an empty string (not 'null'/'undefined')", () => {
		expect(extractCellText(null)).toBe("");
		expect(extractCellText(undefined)).toBe("");
	});

	test("HTML formatter output is stripped to plain text (matches what's visible in the cell)", () => {
		expect(extractCellText("<span>EUR</span>")).toBe("EUR");
		expect(extractCellText('<i class="bi"></i><b>42.5%</b>')).toBe("42.5%");
	});

	test("nested-tag payload is iteratively stripped — CodeQL: js/incomplete-multi-character-sanitization", () => {
		expect(extractCellText("<scr<script>ipt>alert(1)</scr</script>ipt>")).not.toContain("<");
	});

	test("plain object falls back to JSON encoding", () => {
		expect(extractCellText({ a: 1, b: "x" })).toBe('{"a":1,"b":"x"}');
	});

	test("array falls back to JSON encoding", () => {
		expect(extractCellText([1, 2, 3])).toBe("[1,2,3]");
	});

	test("cyclic object is caught and yields some non-throwing string", () => {
		const a = /** @type {any} */ ({});
		a.self = a;
		// Whatever the fallback returns, it must NOT throw — the keyboard handler relies on this.
		expect(() => extractCellText(a)).not.toThrow();
	});
});
