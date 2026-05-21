// @ts-check
import { describe, it, expect, beforeEach } from "vitest";

import {
	aggregateNamesByKind,
	canonicalJson,
	contentHash,
	diffSnapshots,
	emptyStore,
	evictIfNeeded,
	filterColumnsOf,
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
});
