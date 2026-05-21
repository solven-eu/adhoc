// @ts-check
import { describe, it, expect } from "vitest";

import { snapshotToMermaid } from "../src/main/resources/static/ui/js/adhoc-query-history-mermaid.js";

const t0 = Date.UTC(2026, 4, 20, 12, 0, 0);

/**
 * @param {string} id
 * @param {{columnNames?: string[], filterColumnNames?: string[], measureNames?: string[], visitCount?: number, ageMs?: number}} fields
 */
const mkNode = (id, fields = {}) => ({
	id,
	visitCount: fields.visitCount ?? 1,
	lastSeenAt: new Date(t0 - (fields.ageMs ?? 0)).toISOString(),
	columnNames: fields.columnNames || [],
	filterColumnNames: fields.filterColumnNames || [],
	measureNames: fields.measureNames || [],
	queryModelJson: {},
});

describe("snapshotToMermaid", () => {
	it("renders a placeholder for an empty snapshot — mermaid refuses empty graphs", () => {
		const out = snapshotToMermaid(null);
		expect(out.startsWith("graph TD\n")).toBe(true);
		expect(out).toContain("No history yet");
	});

	it("emits one row per entity AND the visit-count chip anchored top-right when count > 1", () => {
		const snap = {
			nodes: {
				abc: mkNode("abc", { columnNames: ["country"], measureNames: ["revenue"], visitCount: 3 }),
			},
			edges: {},
		};
		const out = snapshotToMermaid(snap);
		// Visit-count chip rendered as a styled span, NOT plain "· 3" text.
		expect(out).toContain("adhoc-history-chip--visits");
		expect(out).toContain("×3");
		// Label wrapper is present — CSS pins the chip top-right INSIDE this wrapper via
		// position:absolute/right:0, and reserves padding-right so the chip never overlaps the
		// entity rows. The wrapper also reserves source-order placement of the chip BEFORE the
		// entity rows so the absolute layer fills from the top down.
		expect(out).toContain("adhoc-history-label");
		expect(out.indexOf("adhoc-history-chip--visits")).toBeLessThan(out.indexOf("adhoc-history-entity--col"));
		// One <span> per entity, joined by <br/> — readable multi-row label rather than a
		// comma-separated cram.
		expect(out).toContain("adhoc-history-entity--col");
		expect(out).toContain("#country");
		expect(out).toContain("adhoc-history-entity--measure");
		expect(out).toContain("Σrevenue");
		expect(out).toContain("<br/>");
		// Click handler is registered so the modal can intercept the navigation.
		expect(out).toContain('click n_abc call onHistoryNodeClick("abc")');
	});

	it("renders filter-referenced columns with a distinct chip class", () => {
		const snap = {
			nodes: {
				a: mkNode("a", { columnNames: [], filterColumnNames: ["status"], measureNames: [] }),
			},
			edges: {},
		};
		const out = snapshotToMermaid(snap);
		expect(out).toContain("adhoc-history-entity--filter");
		expect(out).toContain("⊕status");
	});

	it("renders edges with the diff label", () => {
		const snap = {
			nodes: {
				root: mkNode("root", { columnNames: ["country"] }),
				deeper: mkNode("deeper", { columnNames: ["country", "year"] }),
			},
			edges: {
				root: {
					deeper: {
						count: 1,
						lastSeenAt: new Date(t0).toISOString(),
						diff: {
							addedColumns: ["year"],
							removedColumns: [],
							addedMeasures: [],
							removedMeasures: [],
							addedOptions: [],
							removedOptions: [],
							filterChanged: false,
							customMarkersChanged: false,
						},
					},
				},
			},
		};
		const out = snapshotToMermaid(snap);
		expect(out).toContain('n_root -->|"+year"| n_deeper');
	});

	it("highlights the current node via classDef", () => {
		const snap = {
			nodes: {
				abc: mkNode("abc"),
				other: mkNode("other", { ageMs: 1 }),
			},
			edges: {},
		};
		const out = snapshotToMermaid(snap, { currentHash: "abc" });
		expect(out).toContain("class n_abc historyCurrent");
		expect(out).toContain("classDef historyCurrent fill:");
		// The non-current node never gets the highlight class.
		expect(out).not.toContain("class n_other historyCurrent");
	});

	it("respects the direction flag", () => {
		const snap = { nodes: { a: mkNode("a") }, edges: {} };
		expect(snapshotToMermaid(snap, { direction: "LR" }).startsWith("graph LR\n")).toBe(true);
		expect(snapshotToMermaid(snap, { direction: "TD" }).startsWith("graph TD\n")).toBe(true);
	});

	it("labels an empty queryModel node with the empty-entity glyph; no visit chip on count=1", () => {
		const snap = { nodes: { a: mkNode("a", { columnNames: [], measureNames: [], visitCount: 1 }) }, edges: {} };
		const out = snapshotToMermaid(snap);
		expect(out).toContain("adhoc-history-entity--empty");
		expect(out).toContain("∅ empty query");
		// No visit chip when count is 1 — would just add noise.
		expect(out).not.toContain("adhoc-history-chip--visits");
	});

	it("collapses overflow into a 'more' row instead of dropping entities silently", () => {
		// 8 columns, default maxItems=6 → 6 entity rows + one "+2 more" row.
		const many = ["a", "b", "c", "d", "e", "f", "g", "h"];
		const snap = { nodes: { a: mkNode("a", { columnNames: many }) }, edges: {} };
		const out = snapshotToMermaid(snap);
		// First 6 rendered as col chips.
		expect(out).toContain("#a");
		expect(out).toContain("#f");
		// The 7th and 8th roll up into a single more-row rather than being silently dropped.
		expect(out).toContain("adhoc-history-entity--more");
		expect(out).toContain("+2 more");
		expect(out).not.toContain("#g");
		expect(out).not.toContain("#h");
	});
});
