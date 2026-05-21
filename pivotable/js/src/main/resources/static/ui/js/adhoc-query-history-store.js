// @ts-check
//
// Per-cube, persistent (localStorage) graph of queries the user has run on this cube.
// Phase 1: capture only — no UI surfaces consume it yet. Future phases layer autocomplete in
// wizard pickers, forward-suggestion / backtrack chips, and a mermaid "browse history" modal
// on top of the same store shape.
//
// Design highlights:
//   - Strictly per-cube: localStorage key = `pivotable.queryHistory.<cubeId>`. No cross-cube
//     pollution; recommendations stay relevant. (Cross-cube name reuse is explicitly out of
//     scope per the design discussion.)
//   - Node identity = content hash of the canonicalised queryModel JSON. Two visits to the
//     SAME logical query merge into one node with `visitCount++`. Reaching the same node
//     via two paths produces a diamond in the graph — that's the convergence story that
//     makes a graph view more informative than a flat list.
//   - Edges carry a precomputed diff label (`+filter(...)`, `-measure(...)`). Computing once
//     at write time keeps every read fast.
//   - LRU eviction at MAX_NODES per cube. Scored by `visitCount × exp(-Δt/halflife)` so
//     well-loved queries survive long after a noisy one-off exploration.
//   - Outer `schemaVersion: 1` AND per-node `v: 1` — same `v` as
//     `adhoc-query-helper.js#URL_HASH_VERSION` so a future on-wire shape change can migrate
//     or evict legacy nodes deterministically. See `URL_HASH_VERSION` Javadoc for the
//     bump-vs-don't policy.

/** Outer schema version of the localStorage payload. Bumped on incompatible shape changes. */
export const STORE_SCHEMA_VERSION = 1;
/** Per-node `v` stamped at write time. Matches `adhoc-query-helper.js#URL_HASH_VERSION`. */
export const NODE_PAYLOAD_VERSION = 1;
/** Hard cap on nodes per cube before LRU eviction kicks in. ~1 KB/node ⇒ ~5 MB/cube ceiling. */
export const DEFAULT_MAX_NODES = 5000;
/** LRU half-life in milliseconds. Visit-count weight halves every 30 days idle. */
export const DEFAULT_HALFLIFE_MS = 30 * 24 * 60 * 60 * 1000;

/** Object-keys of a queryModel JSON whose array VALUES are order-insensitive for hashing. */
const SORTED_ARRAY_KEYS = new Set(["columns", "measures", "options", "nonPushdowns"]);

/**
 * Canonicalise a value for stable hashing: object keys sorted recursively, designated arrays
 * sorted. Unknown nested arrays (e.g. filter operands) keep their order, since order may carry
 * semantics (an AND/OR tree is not necessarily commutative for human readers, even if logically
 * it is).
 *
 * @param {unknown} value
 * @returns {unknown}
 */
function canonicalize(value) {
	if (Array.isArray(value)) {
		return value.map(canonicalize);
	}
	if (value && typeof value === "object") {
		const out = /** @type {Record<string, unknown>} */ ({});
		const obj = /** @type {Record<string, unknown>} */ (value);
		for (const k of Object.keys(obj).sort()) {
			let v = canonicalize(obj[k]);
			if (SORTED_ARRAY_KEYS.has(k) && Array.isArray(v)) {
				// `[...v].sort()` uses default lexicographic compare — fine for the string-only
				// arrays we sort here (column names, measure names, option names).
				v = [...v].sort();
			}
			out[k] = v;
		}
		return out;
	}
	return value;
}

/**
 * Stable JSON for hashing — `canonicalize` + `JSON.stringify`. Two queryModels that differ
 * only in iteration order of their selectedColumns/selectedMeasures maps (or in
 * lexicographic order of their column/measure arrays) hash to the same string.
 *
 * @param {unknown} parsedJson
 * @returns {string}
 */
export function canonicalJson(parsedJson) {
	return JSON.stringify(canonicalize(parsedJson));
}

/**
 * 53-bit non-cryptographic string hash (cyrb53 — public domain, well-known stable).
 * 53 bits gives ≤ ~1.4e-9 collision probability at 5000 entries — safe headroom over the
 * default `MAX_NODES` cap. We do NOT need crypto-grade hashing here: the hash is a local
 * dedup key, never sent over the wire and never used to authenticate anything.
 *
 * @param {string} str
 * @param {number} [seed]
 * @returns {string} hex digits (≤ 14 chars)
 */
function cyrb53(str, seed = 0) {
	let h1 = 0xdeadbeef ^ seed;
	let h2 = 0x41c6ce57 ^ seed;
	for (let i = 0; i < str.length; i++) {
		const ch = str.charCodeAt(i);
		h1 = Math.imul(h1 ^ ch, 2654435761);
		h2 = Math.imul(h2 ^ ch, 1597334677);
	}
	h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
	h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
	return (4294967296 * (2097151 & h2) + (h1 >>> 0)).toString(16);
}

/**
 * Content-hash of a queryModel parsed JSON.
 *
 * @param {unknown} parsedJson
 * @returns {string}
 */
export function contentHash(parsedJson) {
	return cyrb53(canonicalJson(parsedJson));
}

/**
 * Compute the diff carried on an edge from `fromJson` (parent snapshot) to `toJson` (child).
 * Designed for human-readable edge labels in the mermaid view AND for forward-suggestion chips.
 *
 * <p>The diff is deterministic in array order (added/removed lists are sorted) so identical
 * transitions encountered via different sessions render the same label.
 *
 * @param {Record<string, any> | null | undefined} fromJson
 * @param {Record<string, any>} toJson
 * @returns {{
 *   addedColumns: string[],
 *   removedColumns: string[],
 *   addedMeasures: string[],
 *   removedMeasures: string[],
 *   addedOptions: string[],
 *   removedOptions: string[],
 *   filterChanged: boolean,
 *   customMarkersChanged: boolean
 * }}
 */
export function diffSnapshots(fromJson, toJson) {
	/** @param {any[] | undefined} arr */
	const asSet = (arr) => new Set(arr || []);
	const fromCols = asSet(fromJson?.columns);
	const toCols = asSet(toJson.columns);
	const fromMeas = asSet(fromJson?.measures);
	const toMeas = asSet(toJson.measures);
	const fromOpts = asSet(fromJson?.options);
	const toOpts = asSet(toJson.options);

	/** @param {Set<string>} a @param {Set<string>} b */
	const diff = (a, b) => [...a].filter((x) => !b.has(x)).sort();

	const fromFilter = JSON.stringify(fromJson?.filter || {});
	const toFilter = JSON.stringify(toJson.filter || {});
	const fromMarkers = JSON.stringify(fromJson?.customMarkers || {});
	const toMarkers = JSON.stringify(toJson.customMarkers || {});

	return {
		addedColumns: diff(toCols, fromCols),
		removedColumns: diff(fromCols, toCols),
		addedMeasures: diff(toMeas, fromMeas),
		removedMeasures: diff(fromMeas, toMeas),
		addedOptions: diff(toOpts, fromOpts),
		removedOptions: diff(fromOpts, toOpts),
		filterChanged: fromFilter !== toFilter,
		customMarkersChanged: fromMarkers !== toMarkers,
	};
}

/**
 * Bare-cube initial store shape. Returned when localStorage is empty or schema mismatched.
 *
 * @param {string} cubeId
 * @returns {{ schemaVersion: number, cubeId: string, nodes: Record<string, any>, edges: Record<string, Record<string, any>> }}
 */
export function emptyStore(cubeId) {
	return {
		schemaVersion: STORE_SCHEMA_VERSION,
		cubeId,
		nodes: {},
		edges: {},
	};
}

const STORAGE_KEY_PREFIX = "pivotable.queryHistory.";

/**
 * Storage key for a given cube. Exposed for tests.
 *
 * @param {string} cubeId
 * @returns {string}
 */
export function storageKey(cubeId) {
	return STORAGE_KEY_PREFIX + cubeId;
}

/**
 * Read the per-cube store from a Storage-like backend (defaults to `window.localStorage`).
 * Returns a fresh empty store on any error or schema mismatch — the cache is non-essential,
 * so we never fail user-visible flows because of a parse error here.
 *
 * @param {string} cubeId
 * @param {Storage | null | undefined} [storage]
 * @returns {{ schemaVersion: number, cubeId: string, nodes: Record<string, any>, edges: Record<string, Record<string, any>> }}
 */
export function loadStore(cubeId, storage) {
	const s = storage || (typeof localStorage !== "undefined" ? localStorage : null);
	if (!s) {
		return emptyStore(cubeId);
	}
	try {
		const raw = s.getItem(storageKey(cubeId));
		if (!raw) {
			return emptyStore(cubeId);
		}
		const parsed = JSON.parse(raw);
		if (!parsed || parsed.schemaVersion !== STORE_SCHEMA_VERSION) {
			// Future schema bumps live here as in-place migration arms before this fallback.
			console.info(
				"Discarding query-history store for cube=" + cubeId + ": schemaVersion=" + parsed?.schemaVersion + " (expected " + STORE_SCHEMA_VERSION + ")",
			);
			return emptyStore(cubeId);
		}
		// Defensive: ensure expected shape after a hand-edit or storage corruption.
		parsed.nodes ||= {};
		parsed.edges ||= {};
		return parsed;
	} catch (e) {
		console.warn("Failed to load query-history store for cube=" + cubeId, e);
		return emptyStore(cubeId);
	}
}

/**
 * Persist the per-cube store. Swallows {@code QuotaExceededError} into a warn — the cache is
 * non-essential, and an over-full localStorage is the user's broader problem to solve.
 *
 * @param {string} cubeId
 * @param {object} store
 * @param {Storage | null | undefined} [storage]
 */
export function saveStore(cubeId, store, storage) {
	const s = storage || (typeof localStorage !== "undefined" ? localStorage : null);
	if (!s) {
		return;
	}
	try {
		s.setItem(storageKey(cubeId), JSON.stringify(store));
	} catch (e) {
		console.warn("Failed to save query-history store for cube=" + cubeId, e);
	}
}

/**
 * LRU score: visitCount × 2^(-Δt/halflife). High score = "still relevant". Eviction drops
 * the lowest-scoring nodes first. Pure function — tested in isolation.
 *
 * <p>By construction, a node's score halves after exactly one halflife of inactivity —
 * the intuitive meaning of "halflife". (An earlier draft used `exp(-Δt/halflife)`, which is
 * the time-constant form: same shape but halves at `halflife × ln 2 ≈ 0.69 halflife`.)
 *
 * @param {{ visitCount: number, lastSeenAt: string }} node
 * @param {number} nowMs
 * @param {number} halflifeMs
 */
export function lruScore(node, nowMs, halflifeMs) {
	const lastMs = Date.parse(node.lastSeenAt);
	const ageMs = Math.max(0, nowMs - (Number.isFinite(lastMs) ? lastMs : nowMs));
	return node.visitCount * Math.pow(0.5, ageMs / halflifeMs);
}

/**
 * Evict the lowest-scoring nodes (and their incident edges) until `store.nodes` fits under
 * `maxNodes`. No-op when already under the cap. The current node (if provided) is NEVER
 * evicted — losing the user's "where am I" while they're sitting on that node would be jarring.
 *
 * @param {{ nodes: Record<string, any>, edges: Record<string, Record<string, any>> }} store
 * @param {{ maxNodes: number, nowMs: number, halflifeMs: number, currentHash?: string | null }} opts
 */
export function evictIfNeeded(store, opts) {
	const { maxNodes, nowMs, halflifeMs, currentHash = null } = opts;
	const allIds = Object.keys(store.nodes);
	if (allIds.length <= maxNodes) {
		return;
	}
	const scored = allIds.map((id) => /** @type {[string, number]} */ ([id, lruScore(store.nodes[id], nowMs, halflifeMs)]));
	// Ascending score — drop the worst first.
	scored.sort((a, b) => a[1] - b[1]);
	const toDrop = scored.length - maxNodes;
	let dropped = 0;
	for (const [id] of scored) {
		if (dropped >= toDrop) {
			break;
		}
		if (id === currentHash) {
			continue;
		}
		delete store.nodes[id];
		delete store.edges[id];
		// Also strip incoming edges pointing at the evicted node.
		for (const fromHash of Object.keys(store.edges)) {
			const outs = store.edges[fromHash];
			if (outs && outs[id]) {
				delete outs[id];
				if (Object.keys(outs).length === 0) {
					delete store.edges[fromHash];
				}
			}
		}
		dropped++;
	}
}

/**
 * Idempotent node upsert: increments `visitCount` and refreshes `lastSeenAt` on every call;
 * stamps the immutable fields on first sight.
 *
 * @param {{ nodes: Record<string, any> }} store
 * @param {string} hash
 * @param {Record<string, any>} parsedJson
 * @param {string} nowIso
 */
function upsertNode(store, hash, parsedJson, nowIso) {
	const existing = store.nodes[hash];
	if (existing) {
		existing.visitCount = (existing.visitCount || 0) + 1;
		existing.lastSeenAt = nowIso;
		return existing;
	}
	const columnNames = [...new Set(parsedJson.columns || [])].sort();
	const measureNames = [...new Set(parsedJson.measures || [])].sort();
	const filterColumnNames = filterColumnsOf(parsedJson.filter);
	const node = {
		v: NODE_PAYLOAD_VERSION,
		id: hash,
		queryModelJson: parsedJson,
		visitCount: 1,
		firstSeenAt: nowIso,
		lastSeenAt: nowIso,
		columnNames,
		measureNames,
		filterColumnNames,
		complexity: columnNames.length + measureNames.length + filterColumnNames.length,
	};
	store.nodes[hash] = node;
	return node;
}

/**
 * Best-effort extraction of column names referenced by a filter tree. Recurses on AND/OR-shaped
 * operand arrays; treats any object carrying a `column` field as a leaf. Returns a sorted,
 * de-duplicated array. Used to power the "simpler queries on the same axes" backtrack
 * recommendation (Phase 4) without needing a full filter parser at write time.
 *
 * @param {unknown} filter
 * @returns {string[]}
 */
export function filterColumnsOf(filter) {
	const found = /** @type {Set<string>} */ (new Set());
	const visit = (node) => {
		if (!node || typeof node !== "object") {
			return;
		}
		if (typeof node.column === "string") {
			found.add(node.column);
		}
		if (Array.isArray(node.operands)) {
			node.operands.forEach(visit);
		}
		if (Array.isArray(node.filters)) {
			node.filters.forEach(visit);
		}
	};
	visit(filter);
	return [...found].sort();
}

/**
 * Upsert the edge from→to. Idempotent on identical transitions: increments `count`, refreshes
 * `lastSeenAt`, and recomputes the `diff` only on first sight (diffs are pure functions of the
 * endpoint snapshots, so recomputing on every visit is wasted work).
 *
 * @param {{ edges: Record<string, Record<string, any>>, nodes: Record<string, any> }} store
 * @param {string} fromHash
 * @param {string} toHash
 * @param {string} nowIso
 */
function upsertEdge(store, fromHash, toHash, nowIso) {
	if (fromHash === toHash) {
		// Self-loop: the user re-executed the same query. Not interesting as a graph edge —
		// the visitCount bump on the node already records the re-execution.
		return;
	}
	const outs = (store.edges[fromHash] ||= {});
	const existing = outs[toHash];
	if (existing) {
		existing.count = (existing.count || 0) + 1;
		existing.lastSeenAt = nowIso;
		return existing;
	}
	const edge = {
		count: 1,
		lastSeenAt: nowIso,
		diff: diffSnapshots(store.nodes[fromHash]?.queryModelJson, store.nodes[toHash].queryModelJson),
	};
	outs[toHash] = edge;
	return edge;
}

/**
 * Record a transition from `fromSnapshot` (null on cold-start) to `toSnapshot`. Mutates
 * `store` in place. Pure with respect to time — pass `nowMs` for deterministic testing.
 *
 * @param {{ nodes: Record<string, any>, edges: Record<string, Record<string, any>> }} store
 * @param {Record<string, any> | null} fromSnapshot
 * @param {Record<string, any>} toSnapshot
 * @param {{ nowMs?: number, maxNodes?: number, halflifeMs?: number }} [opts]
 * @returns {{ toHash: string, fromHash: string | null }}
 */
export function recordTransition(store, fromSnapshot, toSnapshot, opts = {}) {
	const nowMs = opts.nowMs ?? Date.now();
	const nowIso = new Date(nowMs).toISOString();
	const maxNodes = opts.maxNodes ?? DEFAULT_MAX_NODES;
	const halflifeMs = opts.halflifeMs ?? DEFAULT_HALFLIFE_MS;

	const toHash = contentHash(toSnapshot);
	upsertNode(store, toHash, toSnapshot, nowIso);

	let fromHash = null;
	if (fromSnapshot) {
		fromHash = contentHash(fromSnapshot);
		// fromHash MUST exist as a node before we can upsert its outgoing edge with a proper
		// diff. Cold-start case is exempt (fromSnapshot is null). For a normal session it
		// has been recorded on the previous transition; for a fresh tab whose cold-start
		// node is `toSnapshot` itself, we never enter this branch.
		if (!store.nodes[fromHash]) {
			upsertNode(store, fromHash, fromSnapshot, nowIso);
		}
		upsertEdge(store, fromHash, toHash, nowIso);
	}

	evictIfNeeded(store, { maxNodes, nowMs, halflifeMs, currentHash: toHash });

	return { toHash, fromHash };
}

/**
 * Aggregate per-name usage scores across the persisted store, partitioned by what each name
 * represents in the queryModel (a groupBy column, a measure, a filter-referenced column).
 *
 * <p>Phase 2 — autocomplete in wizard pickers. The wizard's existing scoring pipeline
 * (`wizardHelper.filtered`) already sorts results by text-match score; the value returned
 * here is fed in as a SECONDARY sort key so personally-frequented entries float to the top
 * of their match tier without ever pushing a worse text-match above a better one.
 *
 * <p>Scoring shape: for each historical node containing the name, contribute
 * {@code node.visitCount × halflifeDecay(node.lastSeenAt)}. Identical to {@link #lruScore} —
 * keeps the "recently and often used" intuition consistent across the eviction and
 * recommendation paths. A name that appears in many queries dominates one that appears in
 * just one; a name not touched for a year decays toward zero.
 *
 * @param {{ nodes: Record<string, any> } | null | undefined} snapshot
 * @param {"column" | "measure" | "filterColumn"} kind
 * @param {{ nowMs?: number, halflifeMs?: number }} [opts]
 * @returns {Map<string, number>} sparse map — names never seen are absent (NOT 0).
 */
export function aggregateNamesByKind(snapshot, kind, opts = {}) {
	/** @type {Map<string, number>} */
	const out = new Map();
	if (!snapshot || !snapshot.nodes) {
		return out;
	}
	const nowMs = opts.nowMs ?? Date.now();
	const halflifeMs = opts.halflifeMs ?? DEFAULT_HALFLIFE_MS;
	const field = kind === "column" ? "columnNames" : kind === "measure" ? "measureNames" : "filterColumnNames";

	for (const node of Object.values(snapshot.nodes)) {
		const names = /** @type {string[] | undefined} */ (node[field]);
		if (!Array.isArray(names) || names.length === 0) {
			continue;
		}
		const weight = lruScore(node, nowMs, halflifeMs);
		if (weight <= 0) {
			continue;
		}
		for (const name of names) {
			out.set(name, (out.get(name) || 0) + weight);
		}
	}
	return out;
}

/**
 * Tiny composable returning a stateful handle that wires the pure store to localStorage +
 * remembers the previous snapshot in a closure. The caller hands it executed queryModel
 * snapshots; the handle does the load → mutate → save dance.
 *
 * <p>Usage in <code>adhoc-query.js</code>:
 * <pre>
 *   const history = useQueryHistoryStore(props.cubeId);
 *   watch(() => tabularView.view, (newView) => {
 *     if (!newView) return;
 *     history.recordExecutedQuery(queryHelper.queryModelToParsedJson(queryModel));
 *   });
 * </pre>
 *
 * @param {string} cubeId
 * @param {{ storage?: Storage | null, maxNodes?: number, halflifeMs?: number }} [opts]
 */
export function useQueryHistoryStore(cubeId, opts = {}) {
	const storage = opts.storage ?? (typeof localStorage !== "undefined" ? localStorage : null);
	const maxNodes = opts.maxNodes ?? DEFAULT_MAX_NODES;
	const halflifeMs = opts.halflifeMs ?? DEFAULT_HALFLIFE_MS;

	/** @type {Record<string, any> | null} */
	let previousSnapshot = null;

	return {
		/**
		 * Record one successfully-executed query. First call on a fresh tab seeds the root
		 * node (no edge); subsequent calls add edges from the previous snapshot.
		 *
		 * @param {Record<string, any>} snapshot a queryModelToParsedJson(queryModel) result
		 * @returns {{ toHash: string, fromHash: string | null }}
		 */
		recordExecutedQuery(snapshot) {
			const store = loadStore(cubeId, storage);
			const result = recordTransition(store, previousSnapshot, snapshot, { maxNodes, halflifeMs });
			saveStore(cubeId, store, storage);
			previousSnapshot = snapshot;
			return result;
		},

		/**
		 * Reset the in-memory "previous node" pointer without touching persisted state.
		 * Useful when the cube changes mid-session: the next executed query becomes a fresh
		 * root in the new cube's DAG.
		 */
		resetPreviousNode() {
			previousSnapshot = null;
		},

		/**
		 * Read-only snapshot of the persisted store. Phase 2+ consumers (autocomplete,
		 * mermaid modal) will call this. Returned object is a deep copy via JSON
		 * round-trip — mutations by the caller do not leak back into storage.
		 */
		snapshot() {
			const store = loadStore(cubeId, storage);
			return JSON.parse(JSON.stringify(store));
		},
	};
}
