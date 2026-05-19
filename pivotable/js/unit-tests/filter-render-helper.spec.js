// @ts-check
import { describe, it, expect } from "vitest";

import { renderValueMatcher } from "../src/main/resources/static/ui/js/adhoc-filter-render-helper.js";

describe("renderValueMatcher", () => {
	it("renders a bare string primitive as = value", () => {
		expect(renderValueMatcher("France")).toEqual({ text: "France", op: "=", strike: false });
	});

	it("renders a number primitive", () => {
		expect(renderValueMatcher(42)).toEqual({ text: "42", op: "=", strike: false });
	});

	it("renders an `equals` matcher as = operand", () => {
		expect(renderValueMatcher({ type: "equals", operand: "France" })).toEqual({ text: "France", op: "=", strike: false });
	});

	it("renders a `not` wrapping a primitive as ≠ + strikethrough", () => {
		expect(renderValueMatcher({ type: "not", negated: "Darla K. Anderson" })).toEqual({
			text: "Darla K. Anderson",
			op: "≠",
			strike: true,
		});
	});

	it("renders a `not(equals)` the same way as `not(primitive)`", () => {
		expect(renderValueMatcher({ type: "not", negated: { type: "equals", operand: "France" } })).toEqual({
			text: "France",
			op: "≠",
			strike: true,
		});
	});

	it("falls back to JSON.stringify for unknown matcher shapes", () => {
		const weird = { type: "like", pattern: "France*" };
		const rendered = renderValueMatcher(weird);
		expect(rendered.op).toBe("=");
		expect(rendered.strike).toBe(false);
		expect(rendered.text).toBe(JSON.stringify(weird));
	});

	it("falls back to JSON.stringify INSIDE a not for deeper shapes", () => {
		const inner = { type: "in", operands: ["a", "b"] };
		const rendered = renderValueMatcher({ type: "not", negated: inner });
		expect(rendered.op).toBe("≠");
		expect(rendered.strike).toBe(true);
		expect(rendered.text).toBe(JSON.stringify(inner));
	});

	it("handles null / undefined defensively", () => {
		expect(renderValueMatcher(null)).toEqual({ text: "", op: "=", strike: false });
		expect(renderValueMatcher(undefined)).toEqual({ text: "", op: "=", strike: false });
		expect(renderValueMatcher({ type: "not", negated: null })).toEqual({ text: "", op: "≠", strike: true });
	});

	it("stringifies object operands inside equals (defensive)", () => {
		const rendered = renderValueMatcher({ type: "equals", operand: { hidden: "object" } });
		expect(rendered.text).toBe('{"hidden":"object"}');
	});
});
