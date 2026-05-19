// @ts-check
import { describe, it, expect } from "vitest";

import { formatViewAge, pickAgeTickMs } from "../src/main/resources/static/ui/js/adhoc-view-age.js";

describe("formatViewAge", () => {
	it("returns 'just now' for sub-second durations", () => {
		expect(formatViewAge(0)).toBe("just now");
		expect(formatViewAge(999)).toBe("just now");
	});

	it("renders seconds up to 120s exclusive", () => {
		expect(formatViewAge(1_000)).toBe("1s ago");
		expect(formatViewAge(60_000)).toBe("60s ago");
		expect(formatViewAge(119_000)).toBe("119s ago");
	});

	it("renders minutes starting at 120s and up to 120 minutes exclusive", () => {
		expect(formatViewAge(120_000)).toBe("2m ago");
		expect(formatViewAge(60 * 60_000)).toBe("60m ago");
		expect(formatViewAge(119 * 60_000)).toBe("119m ago");
	});

	it("renders hours starting at 120 minutes and up to 48 hours exclusive", () => {
		expect(formatViewAge(120 * 60_000)).toBe("2h ago");
		expect(formatViewAge(24 * 3_600_000)).toBe("24h ago");
		expect(formatViewAge(47 * 3_600_000)).toBe("47h ago");
	});

	it("renders days starting at 48 hours", () => {
		expect(formatViewAge(48 * 3_600_000)).toBe("2d ago");
		expect(formatViewAge(7 * 24 * 3_600_000)).toBe("7d ago");
	});

	it("returns the empty string for non-finite or negative inputs", () => {
		expect(formatViewAge(null)).toBe("");
		expect(formatViewAge(undefined)).toBe("");
		expect(formatViewAge(NaN)).toBe("");
		expect(formatViewAge(-1)).toBe("");
	});
});

describe("pickAgeTickMs", () => {
	it("ticks every second under 120s", () => {
		expect(pickAgeTickMs(0)).toBe(1_000);
		expect(pickAgeTickMs(119_000)).toBe(1_000);
	});

	it("ticks every 30 seconds between 120s and 120 minutes", () => {
		expect(pickAgeTickMs(120_000)).toBe(30_000);
		expect(pickAgeTickMs(60 * 60_000)).toBe(30_000);
	});

	it("ticks every minute between 120 minutes and 48 hours", () => {
		expect(pickAgeTickMs(120 * 60_000)).toBe(60_000);
		expect(pickAgeTickMs(24 * 3_600_000)).toBe(60_000);
	});

	it("ticks hourly beyond 48 hours", () => {
		expect(pickAgeTickMs(48 * 3_600_000)).toBe(3_600_000);
		expect(pickAgeTickMs(10 * 24 * 3_600_000)).toBe(3_600_000);
	});
});
