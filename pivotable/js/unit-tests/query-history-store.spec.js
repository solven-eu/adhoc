// @ts-check
import { describe, it, expect, beforeEach } from "vitest";

import {
	aggregateNamesByKind,
	backtrackSuggestions,
	canonicalJson,
	collectDistinctNames,
	contentHash,
	diffLabel,
	diffSnapshots,
	emptyStore,
	evictIfNeeded,
	filterColumnsOf,
	filterSnapshotByNames,
	forgetNode,
	forwardSuggestions,
	loadStore,
	lruScore,
	recordTransition,
	saveStore,
	storageKey,
	useQueryHistoryStore,
	DEFAULT_HALFLIFE_MS,
	NODE_PAYLOAD_VERSION,
	STORE_SCHEMA_VERSION,
} from "../src/main/resources/static/ui/js/adhoc-query-history-store.js";

/**
 * A minimal in-memory Storage stand-in. localStorage is unavailable in the Node-based vitest
 * runner and we want a clean slate per spec anyway.
 *
 * @returns {Storage & { _data: Map<string, string> }}
 */
function makeStubStorage() {
	const data = new Map();
	return /** @type {any} */ ({
		_data: data,
		getItem: (k) => (data.has(k) ? data.get(k) : null),
		setItem: (k, v) => {
			data.set(k, v);
		},
		removeItem: (k) => {
			data.delete(k);
		},
		clear: () => {
			data.clear();
		},
		key: (i) => [...data.keys()][i] ?? null,
		get length() {
			return data.size;
		},
	});
}

describe("canonicalJson", () => {
	it("sorts object keys recursively", () => {
		expect(canonicalJson({ b: 1, a: { z: 2, y: 3 } })).toBe('{"a":{"y":3,"z":2},"b":1}');
	});

	it("sorts known unordered arrays (columns, measures, options, nonPushdowns)", () => {
		const a = { columns: ["b", "a"], measures: ["m2", "m1"], options: ["o2", "o1"], filter: { operands: ["x", "y"] } };
		const b = { columns: ["a", "b"], measures: ["m1", "m2"], options: ["o1", "o2"], filter: { operands: ["x", "y"] } };
		expect(canonicalJson(a)).toBe(canonicalJson(b));
	});

	it("preserves order of unknown-semantic arrays like filter operands", () => {
		// `filter.operands` is NOT in SORTED_ARRAY_KEYS (operand order may matter to humans
		// reading the filter even if logically commutative). Two filters with operands in
		// different orders must hash differently — otherwise we'd merge them visibly
		// when they read differently in the UI.
		const a = { filter: { operands: [{ column: "ccy" }, { column: "color" }] } };
		const b = { filter: { operands: [{ column: "color" }, { column: "ccy" }] } };
		expect(canonicalJson(a)).not.toBe(canonicalJson(b));
	});
});

describe("contentHash", () => {
	it("yields the same hash for equivalent canonical shapes", () => {
		const a = { columns: ["b", "a"], measures: ["m1"] };
		const b = { columns: ["a", "b"], measures: ["m1"] };
		expect(contentHash(a)).toBe(contentHash(b));
	});

	it("yields a different hash when content changes", () => {
		const a = { columns: ["a"], measures: ["m1"] };
		const b = { columns: ["a"], measures: ["m2"] };
		expect(contentHash(a)).not.toBe(contentHash(b));
	});
});

describe("diffSnapshots", () => {
	it("classifies added vs removed columns / measures / options", () => {
		const from = { columns: ["a", "b"], measures: ["m1"], options: ["o1"] };
		const to = { columns: ["b", "c"], measures: ["m2"], options: ["o1", "o2"] };
		const d = diffSnapshots(from, to);
		expect(d.addedColumns).toEqual(["c"]);
		expect(d.removedColumns).toEqual(["a"]);
		expect(d.addedMeasures).toEqual(["m2"]);
		expect(d.removedMeasures).toEqual(["m1"]);
		expect(d.addedOptions).toEqual(["o2"]);
		expect(d.removedOptions).toEqual([]);
		expect(d.filterChanged).toBe(false);
		expect(d.customMarkersChanged).toBe(false);
	});

	it("treats fromJson=null as cold-start (everything in `to` is `added`)", () => {
		const to = { columns: ["a"], measures: ["m1"], options: ["o1"], filter: { x: 1 } };
		const d = diffSnapshots(null, to);
		expect(d.addedColumns).toEqual(["a"]);
		expect(d.addedMeasures).toEqual(["m1"]);
		expect(d.addedOptions).toEqual(["o1"]);
		expect(d.filterChanged).toBe(true);
	});

	it("flags filter / customMarkers changes structurally", () => {
		const from = { columns: [], filter: { column: "ccy", value: "EUR" }, customMarkers: {} };
		const to = { columns: [], filter: { column: "ccy", value: "USD" }, customMarkers: { region: "EU" } };
		const d = diffSnapshots(from, to);
		expect(d.filterChanged).toBe(true);
		expect(d.customMarkersChanged).toBe(true);
	});
});

describe("filterColumnsOf", () => {
	it("returns [] for null / non-object filters", () => {
		expect(filterColumnsOf(null)).toEqual([]);
		expect(filterColumnsOf(undefined)).toEqual([]);
		expect(filterColumnsOf("not-an-object")).toEqual([]);
	});

	it("extracts leaf column names from nested operands / filters arrays", () => {
		const f = {
			operands: [{ column: "ccy", op: "eq" }, { filters: [{ column: "color" }, { column: "ccy" }] }],
		};
		expect(filterColumnsOf(f)).toEqual(["ccy", "color"]);
	});
});

describe("loadStore / saveStore round-trip", () => {
	/** @type {ReturnType<typeof makeStubStorage>} */
	let storage;
	beforeEach(() => {
		storage = makeStubStorage();
	});

	it("returns an empty store when storage is empty", () => {
		const s = loadStore("cubeA", storage);
		expect(s.schemaVersion).toBe(STORE_SCHEMA_VERSION);
		expect(s.cubeId).toBe("cubeA");
		expect(s.nodes).toEqual({});
		expect(s.edges).toEqual({});
	});

	it("persists and re-reads", () => {
		const before = emptyStore("cubeA");
		before.nodes["abc"] = { id: "abc", visitCount: 3 };
		saveStore("cubeA", before, storage);
		const after = loadStore("cubeA", storage);
		expect(after.nodes.abc.visitCount).toBe(3);
	});

	it("discards a payload with mismatched schemaVersion and returns a fresh store", () => {
		storage.setItem(storageKey("cubeA"), JSON.stringify({ schemaVersion: 999, nodes: { x: 1 }, edges: {} }));
		const s = loadStore("cubeA", storage);
		expect(s.schemaVersion).toBe(STORE_SCHEMA_VERSION);
		expect(s.nodes).toEqual({});
	});

	it("survives a corrupt JSON blob (returns empty store)", () => {
		storage.setItem(storageKey("cubeA"), "not-json{");
		expect(loadStore("cubeA", storage).nodes).toEqual({});
	});

	it("keys per-cube", () => {
		const a = emptyStore("cubeA");
		a.nodes["x"] = { id: "x" };
		saveStore("cubeA", a, storage);
		saveStore("cubeB", emptyStore("cubeB"), storage);
		expect(loadStore("cubeA", storage).nodes.x).toBeTruthy();
		expect(loadStore("cubeB", storage).nodes.x).toBeUndefined();
	});
});

describe("recordTransition", () => {
	const t0 = Date.UTC(2026, 4, 20, 12, 0, 0); // deterministic clock

	it("seeds a root node on cold-start (fromSnapshot=null) without an edge", () => {
		const store = emptyStore("cubeA");
		const snap = { columns: ["country"], measures: ["sales"], options: [], filter: {}, customMarkers: {} };
		const { toHash, fromHash } = recordTransition(store, null, snap, { nowMs: t0 });
		expect(fromHash).toBeNull();
		expect(store.nodes[toHash]).toBeTruthy();
		expect(store.nodes[toHash].visitCount).toBe(1);
		expect(store.nodes[toHash].v).toBe(NODE_PAYLOAD_VERSION);
		expect(store.edges).toEqual({});
	});

	it("creates a forward edge with a precomputed diff", () => {
		const store = emptyStore("cubeA");
		const root = { columns: ["country"], measures: ["sales"], options: [], filter: {}, customMarkers: {} };
		const child = { columns: ["country", "year"], measures: ["sales"], options: [], filter: {}, customMarkers: {} };
		const r1 = recordTransition(store, null, root, { nowMs: t0 });
		const r2 = recordTransition(store, root, child, { nowMs: t0 + 1000 });
		expect(store.edges[r1.toHash][r2.toHash].count).toBe(1);
		expect(store.edges[r1.toHash][r2.toHash].diff.addedColumns).toEqual(["year"]);
	});

	it("merges identical-content visits into one node + bumps visitCount + edge count", () => {
		const store = emptyStore("cubeA");
		const root = { columns: ["country"], measures: ["sales"], options: [], filter: {}, customMarkers: {} };
		const child = { columns: ["country", "year"], measures: ["sales"], options: [], filter: {}, customMarkers: {} };
		recordTransition(store, null, root, { nowMs: t0 });
		const a = recordTransition(store, root, child, { nowMs: t0 + 1000 });
		const b = recordTransition(store, root, child, { nowMs: t0 + 2000 });
		expect(a.toHash).toBe(b.toHash);
		expect(store.nodes[a.toHash].visitCount).toBe(2);
		expect(store.edges[a.fromHash][a.toHash].count).toBe(2);
	});

	it("converges on the same destination when reached via two distinct paths (diamond)", () => {
		const store = emptyStore("cubeA");
		const root = { columns: [], measures: [], options: [], filter: {}, customMarkers: {} };
		const viaA = { columns: ["a"], measures: [], options: [], filter: {}, customMarkers: {} };
		const viaB = { columns: ["b"], measures: [], options: [], filter: {}, customMarkers: {} };
		const both = { columns: ["a", "b"], measures: [], options: [], filter: {}, customMarkers: {} };
		recordTransition(store, null, root, { nowMs: t0 });
		recordTransition(store, root, viaA, { nowMs: t0 + 1000 });
		recordTransition(store, viaA, both, { nowMs: t0 + 2000 });
		recordTransition(store, viaB, both, { nowMs: t0 + 3000 });
		// `both` node exists exactly once (content-hash dedup) — and is reachable via both
		// viaA and viaB. THIS is the convergence story that makes a graph view earn its keep.
		const bothHash = contentHash(both);
		expect(store.nodes[bothHash]).toBeTruthy();
		expect(store.edges[contentHash(viaA)][bothHash]).toBeTruthy();
		expect(store.edges[contentHash(viaB)][bothHash]).toBeTruthy();
	});

	it("does not create a self-loop edge when re-executing the same query", () => {
		const store = emptyStore("cubeA");
		const q = { columns: ["a"], measures: [], options: [], filter: {}, customMarkers: {} };
		const r1 = recordTransition(store, null, q, { nowMs: t0 });
		const r2 = recordTransition(store, q, q, { nowMs: t0 + 1000 });
		expect(r1.toHash).toBe(r2.toHash);
		expect(store.edges[r1.toHash]).toBeUndefined();
		expect(store.nodes[r1.toHash].visitCount).toBe(2);
	});
});

describe("evictIfNeeded", () => {
	const t0 = Date.UTC(2026, 4, 20, 12, 0, 0);

	const mkNode = (id, visitCount, ageMs) => ({
		id,
		visitCount,
		lastSeenAt: new Date(t0 - ageMs).toISOString(),
	});

	it("is a no-op when under cap", () => {
		const store = { nodes: { a: mkNode("a", 1, 0), b: mkNode("b", 1, 0) }, edges: {} };
		evictIfNeeded(store, { maxNodes: 5, nowMs: t0, halflifeMs: DEFAULT_HALFLIFE_MS });
		expect(Object.keys(store.nodes)).toHaveLength(2);
	});

	it("drops the lowest-scoring nodes until under cap", () => {
		const store = {
			nodes: {
				old_unused: mkNode("old_unused", 1, 365 * 24 * 60 * 60 * 1000), // 1 year old, 1 visit
				old_loved: mkNode("old_loved", 100, 365 * 24 * 60 * 60 * 1000), // 1 year old but heavily used
				new_unused: mkNode("new_unused", 1, 1000), // 1 second old, 1 visit
			},
			edges: {},
		};
		evictIfNeeded(store, { maxNodes: 2, nowMs: t0, halflifeMs: DEFAULT_HALFLIFE_MS });
		expect(store.nodes.old_unused).toBeUndefined(); // dropped — both old and unused
		expect(store.nodes.new_unused).toBeTruthy(); // kept — recent
		expect(store.nodes.old_loved).toBeTruthy(); // kept — high visitCount survives age decay
	});

	it("never evicts the currentHash even if it scores worst", () => {
		const store = {
			nodes: {
				current_one_visit: mkNode("current_one_visit", 1, 0),
				popular: mkNode("popular", 50, 1000),
				ancient: mkNode("ancient", 50, 365 * 24 * 60 * 60 * 1000),
			},
			edges: {},
		};
		evictIfNeeded(store, {
			maxNodes: 2,
			nowMs: t0,
			halflifeMs: DEFAULT_HALFLIFE_MS,
			currentHash: "current_one_visit",
		});
		expect(store.nodes.current_one_visit).toBeTruthy();
	});

	it("cascades edge removal (outgoing AND incoming) for evicted nodes", () => {
		// `a` and `b` are tied on score (same visitCount, same age); `c` dominates. With
		// Array.sort being stable in V8 and Object.keys preserving insertion order, the
		// first tied entry (`a`) is dropped before the second (`b`). The test asserts
		// against that specific eviction so the cascade logic gets exercised on a node
		// that participates in both an incoming AND an outgoing edge.
		const store = {
			nodes: { a: mkNode("a", 1, 0), b: mkNode("b", 1, 0), c: mkNode("c", 99, 0) },
			edges: {
				a: { b: { count: 1 } }, // outgoing edge of `a` to `b`
				b: { c: { count: 1 } }, // outgoing edge of `b` (still alive after `a` drops)
			},
		};
		evictIfNeeded(store, { maxNodes: 2, nowMs: t0, halflifeMs: DEFAULT_HALFLIFE_MS });
		expect(store.nodes.a).toBeUndefined();
		// a's outgoing edges (to b) removed as part of the cascade
		expect(store.edges.a).toBeUndefined();
		// b survived; its outgoing edge to c is still intact
		expect(store.nodes.b).toBeTruthy();
		expect(store.edges.b?.c).toBeTruthy();
	});

	it("strips INCOMING edges to an evicted node even when the source node survives", () => {
		// `a` and `c` are loved; `b` is evicted. The edge a→b must disappear so dangling
		// edges don't accumulate over time. (Same scenario, different angle from above.)
		const store = {
			nodes: { a: mkNode("a", 99, 0), b: mkNode("b", 1, 100 * DEFAULT_HALFLIFE_MS), c: mkNode("c", 99, 0) },
			edges: {
				a: { b: { count: 1 } },
				b: { c: { count: 1 } },
			},
		};
		evictIfNeeded(store, { maxNodes: 2, nowMs: t0, halflifeMs: DEFAULT_HALFLIFE_MS });
		expect(store.nodes.b).toBeUndefined();
		expect(store.edges.a?.b).toBeUndefined();
		expect(store.edges.b).toBeUndefined();
	});
});

describe("lruScore", () => {
	const t0 = Date.UTC(2026, 4, 20, 12, 0, 0);

	it("is monotonic decreasing in age and increasing in visitCount", () => {
		const halflife = DEFAULT_HALFLIFE_MS;
		const fresh = { visitCount: 1, lastSeenAt: new Date(t0).toISOString() };
		const old = { visitCount: 1, lastSeenAt: new Date(t0 - halflife).toISOString() };
		expect(lruScore(fresh, t0, halflife)).toBeGreaterThan(lruScore(old, t0, halflife));
		const loved = { visitCount: 10, lastSeenAt: new Date(t0).toISOString() };
		expect(lruScore(loved, t0, halflife)).toBeGreaterThan(lruScore(fresh, t0, halflife));
	});

	it("halves the score after one halflife of inactivity", () => {
		const halflife = DEFAULT_HALFLIFE_MS;
		const fresh = { visitCount: 4, lastSeenAt: new Date(t0).toISOString() };
		const old = { visitCount: 4, lastSeenAt: new Date(t0 - halflife).toISOString() };
		expect(lruScore(fresh, t0, halflife)).toBeCloseTo(2 * lruScore(old, t0, halflife), 6);
	});
});

describe("aggregateNamesByKind", () => {
	const t0 = Date.UTC(2026, 4, 20, 12, 0, 0);

	/** @param {string} id @param {number} visitCount @param {number} ageMs @param {Record<string, string[]>} fields */
	const mkNode = (id, visitCount, ageMs, fields) => ({
		id,
		visitCount,
		lastSeenAt: new Date(t0 - ageMs).toISOString(),
		columnNames: fields.columnNames || [],
		measureNames: fields.measureNames || [],
		filterColumnNames: fields.filterColumnNames || [],
	});

	it("returns an empty Map for null / empty snapshots", () => {
		expect(aggregateNamesByKind(null, "column").size).toBe(0);
		expect(aggregateNamesByKind(undefined, "measure").size).toBe(0);
		expect(aggregateNamesByKind({ nodes: {} }, "column").size).toBe(0);
	});

	it("sums visit weights across nodes for the same name (column kind)", () => {
		const snap = {
			nodes: {
				n1: mkNode("n1", 3, 0, { columnNames: ["country", "year"] }),
				n2: mkNode("n2", 7, 0, { columnNames: ["country"] }),
				n3: mkNode("n3", 2, 0, { columnNames: ["city"] }),
			},
		};
		const scores = aggregateNamesByKind(snap, "column", { nowMs: t0 });
		// All nodes age=0 so the weight === visitCount.
		expect(scores.get("country")).toBe(10); // 3 + 7
		expect(scores.get("year")).toBe(3);
		expect(scores.get("city")).toBe(2);
	});

	it("partitions by kind — only the requested field contributes", () => {
		const snap = {
			nodes: {
				n1: mkNode("n1", 5, 0, { columnNames: ["foo"], measureNames: ["bar"] }),
			},
		};
		expect([...aggregateNamesByKind(snap, "column", { nowMs: t0 })]).toEqual([["foo", 5]]);
		expect([...aggregateNamesByKind(snap, "measure", { nowMs: t0 })]).toEqual([["bar", 5]]);
		expect([...aggregateNamesByKind(snap, "filterColumn", { nowMs: t0 })]).toEqual([]);
	});

	it("decays by halflife — a node one halflife old contributes half-weight", () => {
		const snap = {
			nodes: {
				fresh: mkNode("fresh", 4, 0, { columnNames: ["country"] }),
				old: mkNode("old", 4, DEFAULT_HALFLIFE_MS, { columnNames: ["country"] }),
			},
		};
		const scores = aggregateNamesByKind(snap, "column", { nowMs: t0 });
		// fresh contributes 4, old contributes 4*0.5=2, total = 6
		expect(scores.get("country")).toBeCloseTo(6, 6);
	});

	it("never seeds 0 entries — names absent from history simply don't appear", () => {
		const snap = { nodes: { n1: mkNode("n1", 1, 0, { columnNames: ["country"] }) } };
		const scores = aggregateNamesByKind(snap, "column", { nowMs: t0 });
		expect(scores.has("nonexistent")).toBe(false);
	});
});

describe("useQueryHistoryStore composable", () => {
	it("seeds a root node on first call, then adds an edge on the second", () => {
		const storage = makeStubStorage();
		const h = useQueryHistoryStore("cubeA", { storage });
		const q1 = { columns: ["a"], measures: ["m"], options: [], filter: {}, customMarkers: {} };
		const q2 = { columns: ["a", "b"], measures: ["m"], options: [], filter: {}, customMarkers: {} };
		const r1 = h.recordExecutedQuery(q1);
		const r2 = h.recordExecutedQuery(q2);
		expect(r1.fromHash).toBeNull();
		expect(r2.fromHash).toBe(r1.toHash);
		const snap = h.snapshot();
		expect(Object.keys(snap.nodes)).toHaveLength(2);
		expect(snap.edges[r2.fromHash][r2.toHash].diff.addedColumns).toEqual(["b"]);
	});

	it("snapshot() returns a deep copy — mutations by the caller do not leak back", () => {
		const storage = makeStubStorage();
		const h = useQueryHistoryStore("cubeA", { storage });
		h.recordExecutedQuery({ columns: ["a"], measures: [], options: [], filter: {}, customMarkers: {} });
		const s1 = h.snapshot();
		const onlyId = Object.keys(s1.nodes)[0];
		s1.nodes[onlyId].visitCount = 999;
		const s2 = h.snapshot();
		expect(s2.nodes[onlyId].visitCount).toBe(1);
	});

	it("resetPreviousNode() makes the next executed query a fresh root (no edge)", () => {
		const storage = makeStubStorage();
		const h = useQueryHistoryStore("cubeA", { storage });
		h.recordExecutedQuery({ columns: ["a"], measures: [], options: [], filter: {}, customMarkers: {} });
		h.resetPreviousNode();
		const r2 = h.recordExecutedQuery({ columns: ["b"], measures: [], options: [], filter: {}, customMarkers: {} });
		expect(r2.fromHash).toBeNull();
		const snap = h.snapshot();
		expect(snap.edges).toEqual({});
	});

	it("forget(hash) drops the node and its incident edges; persists", () => {
		const storage = makeStubStorage();
		const h = useQueryHistoryStore("cubeA", { storage });
		const q1 = { columns: ["a"], measures: [], options: [], filter: {}, customMarkers: {} };
		const q2 = { columns: ["a", "b"], measures: [], options: [], filter: {}, customMarkers: {} };
		const r1 = h.recordExecutedQuery(q1);
		const r2 = h.recordExecutedQuery(q2);
		h.forget(r2.toHash);
		const snap = h.snapshot();
		expect(snap.nodes[r2.toHash]).toBeUndefined();
		expect(snap.edges[r1.toHash]?.[r2.toHash]).toBeUndefined();
		// And the next process re-reads the persisted (forgotten) state — not just an in-memory wipe.
		const reload = useQueryHistoryStore("cubeA", { storage });
		expect(reload.snapshot().nodes[r2.toHash]).toBeUndefined();
	});

	it("clearAll() empties the persisted graph and resets previous-node", () => {
		const storage = makeStubStorage();
		const h = useQueryHistoryStore("cubeA", { storage });
		h.recordExecutedQuery({ columns: ["a"], measures: [], options: [], filter: {}, customMarkers: {} });
		h.recordExecutedQuery({ columns: ["a", "b"], measures: [], options: [], filter: {}, customMarkers: {} });
		h.clearAll();
		const after = h.snapshot();
		expect(after.nodes).toEqual({});
		// And subsequent capture restarts as a root (no edge — previousSnapshot was wiped).
		const r3 = h.recordExecutedQuery({ columns: ["c"], measures: [], options: [], filter: {}, customMarkers: {} });
		expect(r3.fromHash).toBeNull();
	});
});

// ---------------------------------------------------------------------------------------------
// diffLabel — short edge / suggestion labels for the mermaid view + Phase 4 chips.
// ---------------------------------------------------------------------------------------------
describe("diffLabel", () => {
	it("returns '' for an empty / null diff", () => {
		expect(diffLabel(null)).toBe("");
		expect(
			diffLabel({
				addedColumns: [],
				removedColumns: [],
				addedMeasures: [],
				removedMeasures: [],
				addedOptions: [],
				removedOptions: [],
				filterChanged: false,
				customMarkersChanged: false,
			}),
		).toBe("");
	});

	it("formats additions with + and removals with − (default 'both')", () => {
		const d = {
			addedColumns: ["country"],
			removedColumns: ["city"],
			addedMeasures: ["revenue"],
			removedMeasures: [],
			addedOptions: [],
			removedOptions: [],
			filterChanged: false,
			customMarkersChanged: false,
		};
		expect(diffLabel(d)).toBe("+country, +revenue, −city");
	});

	it("direction='added' suppresses removals (forward chip)", () => {
		const d = {
			addedColumns: ["country"],
			removedColumns: ["city"],
			addedMeasures: [],
			removedMeasures: [],
			addedOptions: [],
			removedOptions: [],
			filterChanged: false,
			customMarkersChanged: false,
		};
		expect(diffLabel(d, { direction: "added" })).toBe("+country");
	});

	it("direction='removed' shows what would peel away (backtrack chip)", () => {
		const d = {
			addedColumns: [],
			removedColumns: ["color", "year"],
			addedMeasures: [],
			removedMeasures: [],
			addedOptions: [],
			removedOptions: [],
			filterChanged: false,
			customMarkersChanged: false,
		};
		expect(diffLabel(d, { direction: "removed" })).toBe("−color, −year");
	});

	it("truncates with ellipsis when exceeding maxItems", () => {
		const d = {
			addedColumns: ["a", "b", "c", "d", "e", "f"],
			removedColumns: [],
			addedMeasures: [],
			removedMeasures: [],
			addedOptions: [],
			removedOptions: [],
			filterChanged: false,
			customMarkersChanged: false,
		};
		expect(diffLabel(d, { maxItems: 3 })).toBe("+a, +b, +c, …");
	});
});

// ---------------------------------------------------------------------------------------------
// forwardSuggestions — Phase 4 forward chips.
// ---------------------------------------------------------------------------------------------
describe("forwardSuggestions", () => {
	const t0 = Date.UTC(2026, 4, 20, 12, 0, 0);

	it("returns [] when current node has no outgoing edges", () => {
		const snap = {
			nodes: { a: { id: "a", visitCount: 1, lastSeenAt: new Date(t0).toISOString(), columnNames: [], measureNames: [], filterColumnNames: [] } },
			edges: {},
		};
		expect(forwardSuggestions(snap, "a", { nowMs: t0 })).toEqual([]);
	});

	it("ranks outgoing edges by edge count × halflife decay", () => {
		const mkNode = (id, parsedJson = {}) => ({
			id,
			visitCount: 1,
			lastSeenAt: new Date(t0).toISOString(),
			columnNames: [],
			measureNames: [],
			filterColumnNames: [],
			queryModelJson: parsedJson,
		});
		const mkEdge = (count, ageMs, diff) => ({
			count,
			lastSeenAt: new Date(t0 - ageMs).toISOString(),
			diff,
		});
		const snap = {
			nodes: {
				a: mkNode("a"),
				b: mkNode("b", { columns: ["country"] }),
				c: mkNode("c", { columns: ["year"] }),
				d: mkNode("d", { columns: ["color"] }),
			},
			edges: {
				a: {
					b: mkEdge(10, 0, {
						addedColumns: ["country"],
						addedMeasures: [],
						addedOptions: [],
						removedColumns: [],
						removedMeasures: [],
						removedOptions: [],
						filterChanged: false,
						customMarkersChanged: false,
					}),
					c: mkEdge(20, DEFAULT_HALFLIFE_MS, {
						addedColumns: ["year"],
						addedMeasures: [],
						addedOptions: [],
						removedColumns: [],
						removedMeasures: [],
						removedOptions: [],
						filterChanged: false,
						customMarkersChanged: false,
					}), // 20 * 0.5 = 10 → tied with b on score, but lexicographic on toHash breaks via sort stability
					d: mkEdge(1, 0, {
						addedColumns: ["color"],
						addedMeasures: [],
						addedOptions: [],
						removedColumns: [],
						removedMeasures: [],
						removedOptions: [],
						filterChanged: false,
						customMarkersChanged: false,
					}),
				},
			},
		};
		const out = forwardSuggestions(snap, "a", { nowMs: t0, topN: 3 });
		expect(out.map((c) => c.toHash)).toEqual(["b", "c", "d"]); // b/c both score 10, d scores 1; insertion order preserved for the tie
		expect(out[0].label).toBe("+country");
		expect(out[0].target.queryModelJson).toEqual({ columns: ["country"] });
	});

	it("skips dangling edges (target node already evicted)", () => {
		const snap = {
			nodes: { a: { id: "a", visitCount: 1, lastSeenAt: new Date(t0).toISOString(), columnNames: [], measureNames: [], filterColumnNames: [] } },
			edges: {
				a: {
					gone: {
						count: 5,
						lastSeenAt: new Date(t0).toISOString(),
						diff: {
							addedColumns: ["x"],
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
		expect(forwardSuggestions(snap, "a", { nowMs: t0 })).toEqual([]);
	});

	it("respects topN", () => {
		const mkNode = (id) => ({
			id,
			visitCount: 1,
			lastSeenAt: new Date(t0).toISOString(),
			columnNames: [],
			measureNames: [],
			filterColumnNames: [],
			queryModelJson: {},
		});
		const mkEdge = (count) => ({
			count,
			lastSeenAt: new Date(t0).toISOString(),
			diff: {
				addedColumns: ["x"],
				removedColumns: [],
				addedMeasures: [],
				removedMeasures: [],
				addedOptions: [],
				removedOptions: [],
				filterChanged: false,
				customMarkersChanged: false,
			},
		});
		const snap = {
			nodes: { a: mkNode("a"), b: mkNode("b"), c: mkNode("c"), d: mkNode("d"), e: mkNode("e") },
			edges: { a: { b: mkEdge(4), c: mkEdge(3), d: mkEdge(2), e: mkEdge(1) } },
		};
		expect(forwardSuggestions(snap, "a", { nowMs: t0, topN: 2 }).map((c) => c.toHash)).toEqual(["b", "c"]);
	});
});

// ---------------------------------------------------------------------------------------------
// backtrackSuggestions — Phase 4 backtrack chips.
// ---------------------------------------------------------------------------------------------
describe("backtrackSuggestions", () => {
	const t0 = Date.UTC(2026, 4, 20, 12, 0, 0);

	const mkNode = (id, visitCount, ageMs, fields, parsedJson = {}) => ({
		id,
		visitCount,
		lastSeenAt: new Date(t0 - ageMs).toISOString(),
		columnNames: fields.columnNames || [],
		measureNames: fields.measureNames || [],
		filterColumnNames: fields.filterColumnNames || [],
		queryModelJson: parsedJson,
	});

	it("returns [] when current node is missing from the snapshot", () => {
		expect(backtrackSuggestions({ nodes: {} }, "missing", { nowMs: t0 })).toEqual([]);
	});

	it("returns [] when nothing is a strict subset of current", () => {
		const snap = {
			nodes: {
				current: mkNode("current", 1, 0, { columnNames: ["a"] }),
				wider: mkNode("wider", 1, 0, { columnNames: ["a", "b"] }), // SUPERSET — not a backtrack
				sideways: mkNode("sideways", 1, 0, { columnNames: ["b"] }), // overlap but not subset
			},
		};
		expect(backtrackSuggestions(snap, "current", { nowMs: t0 })).toEqual([]);
	});

	it("returns strict subsets ranked by visitCount × recency", () => {
		const snap = {
			nodes: {
				current: mkNode("current", 1, 0, { columnNames: ["a", "b"], measureNames: ["m"] }),
				dropB: mkNode("dropB", 5, 0, { columnNames: ["a"], measureNames: ["m"] }),
				dropAll: mkNode("dropAll", 99, 0, { columnNames: [], measureNames: [] }),
				dropAllOld: mkNode("dropAllOld", 99, 365 * 24 * 60 * 60 * 1000, { columnNames: [], measureNames: [] }), // ancient — visitCount alone shouldn't win
				selfHashed: mkNode("current", 1, 0, { columnNames: ["a", "b"] }), // same hash as current — must be skipped
			},
		};
		const out = backtrackSuggestions(snap, "current", { nowMs: t0, topN: 3 });
		const hashes = out.map((c) => c.toHash);
		expect(hashes).toContain("dropAll"); // 99 visits, fresh → highest score
		expect(hashes).toContain("dropB"); // 5 visits, fresh
		// dropAllOld has 99 visits but is one year old → score halved many times over. Should still
		// be in the top-3 here (the test passes topN: 3 — only 3 distinct candidates exist beyond
		// 'current' itself), but ranked LAST.
		expect(out[0].toHash).toBe("dropAll"); // confirmed top
		expect(hashes).not.toContain("current"); // self-skip
	});

	it("labels the chip as what would peel away (negative direction)", () => {
		const snap = {
			nodes: {
				current: mkNode("current", 1, 0, { columnNames: ["a", "b"] }),
				dropB: mkNode("dropB", 1, 0, { columnNames: ["a"] }),
			},
		};
		const out = backtrackSuggestions(snap, "current", { nowMs: t0 });
		expect(out[0].label).toBe("−b");
	});
});

describe("forgetNode (pure)", () => {
	it("is idempotent on unknown hash", () => {
		const store = { nodes: {}, edges: {} };
		forgetNode(store, "nope");
		expect(store.nodes).toEqual({});
		expect(store.edges).toEqual({});
	});

	it("removes node + outgoing + incoming edges", () => {
		const store = {
			nodes: { a: { id: "a" }, b: { id: "b" }, c: { id: "c" } },
			edges: {
				a: { b: { count: 1 } },
				b: { c: { count: 1 } },
			},
		};
		forgetNode(store, "b");
		expect(store.nodes.b).toBeUndefined();
		expect(store.edges.a?.b).toBeUndefined();
		expect(store.edges.b).toBeUndefined();
		// 'a' has no more outgoing edges → its key was reaped to keep the structure tidy.
		expect(store.edges.a).toBeUndefined();
	});
});

// ---------------------------------------------------------------------------------------------
// collectDistinctNames — chip universe.
// ---------------------------------------------------------------------------------------------
describe("collectDistinctNames", () => {
	const mkNode = (id, fields = {}) => ({
		id,
		visitCount: 1,
		lastSeenAt: "2026-05-21T00:00:00.000Z",
		columnNames: fields.columnNames || [],
		measureNames: fields.measureNames || [],
		filterColumnNames: fields.filterColumnNames || [],
	});

	it("returns empty arrays for null / empty snapshots", () => {
		expect(collectDistinctNames(null)).toEqual({ columns: [], measures: [] });
		expect(collectDistinctNames({ nodes: {} })).toEqual({ columns: [], measures: [] });
	});

	it("unions columnNames + filterColumnNames into a single columns axis", () => {
		const snap = {
			nodes: {
				a: mkNode("a", { columnNames: ["country"], filterColumnNames: ["status"] }),
				b: mkNode("b", { columnNames: ["country", "year"] }),
			},
		};
		expect(collectDistinctNames(snap).columns).toEqual(["country", "status", "year"]);
	});

	it("collects measures separately, sorted, de-duplicated", () => {
		const snap = {
			nodes: {
				a: mkNode("a", { measureNames: ["revenue", "count"] }),
				b: mkNode("b", { measureNames: ["revenue"] }),
			},
		};
		expect(collectDistinctNames(snap).measures).toEqual(["count", "revenue"]);
	});
});

// ---------------------------------------------------------------------------------------------
// filterSnapshotByNames — tri-state chip filter applied to the snapshot.
// ---------------------------------------------------------------------------------------------
describe("filterSnapshotByNames", () => {
	const mkNode = (id, fields = {}) => ({
		id,
		visitCount: 1,
		lastSeenAt: "2026-05-21T00:00:00.000Z",
		columnNames: fields.columnNames || [],
		measureNames: fields.measureNames || [],
		filterColumnNames: fields.filterColumnNames || [],
	});
	const baseSnap = {
		nodes: {
			a: mkNode("a", { columnNames: ["country"], measureNames: ["revenue"] }),
			b: mkNode("b", { columnNames: ["country", "year"], measureNames: ["revenue"] }),
			c: mkNode("c", { columnNames: ["city"], measureNames: ["count"] }),
		},
		edges: {
			a: { b: { count: 1 } },
			b: { c: { count: 1 } },
		},
	};

	it("identity when no filter is applied", () => {
		const out = filterSnapshotByNames(baseSnap, {});
		expect(Object.keys(out.nodes).sort()).toEqual(["a", "b", "c"]);
		expect(out.edges.a.b).toBeTruthy();
	});

	it("includeColumns: keeps only nodes that reference EVERY required column", () => {
		const out = filterSnapshotByNames(baseSnap, { includeColumns: ["country"] });
		expect(Object.keys(out.nodes).sort()).toEqual(["a", "b"]);
		// 'c' didn't survive → the b→c edge must also be stripped (orphan-edge prevention).
		expect(out.edges.b?.c).toBeUndefined();
		// a→b survived (both endpoints present).
		expect(out.edges.a?.b).toBeTruthy();
	});

	it("excludeColumns: hides nodes that reference any forbidden column", () => {
		const out = filterSnapshotByNames(baseSnap, { excludeColumns: ["city"] });
		expect(Object.keys(out.nodes).sort()).toEqual(["a", "b"]);
		expect(out.edges.b?.c).toBeUndefined();
	});

	it("includeMeasures + excludeMeasures combine multiplicatively", () => {
		const out = filterSnapshotByNames(baseSnap, { includeMeasures: ["revenue"], excludeMeasures: ["count"] });
		// 'a' and 'b' have revenue and no count → in. 'c' has count and no revenue → out (both).
		expect(Object.keys(out.nodes).sort()).toEqual(["a", "b"]);
	});

	it("filterColumnNames count toward the columns axis", () => {
		const snap = {
			nodes: {
				a: mkNode("a", { columnNames: [], filterColumnNames: ["country"] }),
			},
			edges: {},
		};
		// 'a' references country only via its filter — include should still admit it.
		expect(Object.keys(filterSnapshotByNames(snap, { includeColumns: ["country"] }).nodes)).toEqual(["a"]);
	});

	it("include AND exclude on the same column reduces to exclude (intersection is empty)", () => {
		const out = filterSnapshotByNames(baseSnap, { includeColumns: ["country"], excludeColumns: ["country"] });
		expect(Object.keys(out.nodes)).toEqual([]);
	});

	it("does not mutate the input snapshot", () => {
		const before = JSON.stringify(baseSnap);
		filterSnapshotByNames(baseSnap, { includeColumns: ["country"] });
		expect(JSON.stringify(baseSnap)).toBe(before);
	});
});
