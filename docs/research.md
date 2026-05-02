# Research

- Boolean Algebra / Logic optimization https://en.m.wikipedia.org/wiki/Logic_optimization
- https://en.m.wikipedia.org/wiki/Quine%E2%80%93McCluskey_algorithm
- https://en.m.wikipedia.org/wiki/Espresso_heuristic_logic_minimizer
- Algebraic factoring / Logic Synthesis / Kernel Extraction / Common Subexpression Extraction (CSE): merging table steps leads to large OR, which should be simplied before being submitted to `ITableWrapper`.
- Task dependencies (as DAG) optimization (context: ICuboid execution and inference)
- Dependancy construction: all table steps may be inferred by a smaller set of input steps.
- Merging of leaves into new leaves (e.g. leading to less queries to ITableWrapper)
- Generating intermediate nodes computing shared computation (e.g. applying `a=a1` once even if needed by multiple steps)
- Columnar Random-Access Compression
- FSST enables random-access over compressed `List` of `String`
- Immutable, Sequenced, Perfect-Hash `Map` (`IAdhocMap`)
- **Immutable**: no defensive copies needed; safe as `HashMap` key; enables hashCode caching (à la `String.hashCode()`).
- **`SequencedMap`** (Java 21): insertion order is preserved and exposed, unlike `HashMap`. Keys and values can be iterated in a stable, predictable order without sorting overhead.
- **Faster `.get` via perfect hashing** (`SimplePerfectHash`): for a fixed, known key-set the hash function maps every key to a unique slot with no collision, so lookup is a single array access after one multiply+mask rather than open-addressing or chaining.
- **`Comparable`**: enables `TreeMap`/sorted-column optimisations and benefits from JEP 180 (tree-ified `HashMap` collision chains use `compareTo` when keys implement `Comparable`).
- **Cached keyset projections** (`retainAll`): the mapping from a full keyset to a projected sub-keyset is memoised, so repeated `retainAll` calls (common in groupBy/slice projection) pay the cost only once.
- State-of-the-art alternatives worth knowing:
- [Guava `ImmutableMap`](https://github.com/google/guava/wiki/ImmutableCollectionsExplained) — immutable, iteration-ordered, array-backed for ≤ 5 entries; no `SequencedMap`, no perfect hash.
- [Eclipse Collections `UnifiedMap` / immutable maps](https://github.com/eclipse/eclipse-collections) — rich immutable variants, primitive-key maps, lower memory footprint than `HashMap`.
- [sux4j](https://github.com/vigna/sux4j) (Sebastiano Vigna) — minimal perfect hash functions (MPHF) for static key sets; the theoretical gold standard for space-optimal MPHFs.
- [CHD algorithm](http://cmph.sourceforge.net/) (Compress, Hash, Displace) — near-optimal MPHF construction; also implemented in [CMPH](http://cmph.sourceforge.net/).
- [Abseil `flat_hash_map` / Swiss tables](https://abseil.io/about/design/swisstables) — SIMD-accelerated open-addressing with a separate metadata byte array; sets the bar for CPU-cache-friendly hash-map throughput (C++; inspiration for Java ports like [F14](https://github.com/facebook/folly/blob/main/folly/container/F14.md)).
- [HPPC](https://github.com/carrotsearch/hppc) (High Performance Primitive Collections) — avoids boxing for primitive keys/values; relevant when keys are `int`/`long` rather than `String`.

## Joining Cuboids

A recurring operation in Adhoc is **joining cuboids**: when a derived measure (e.g. a `Combinator`) depends on N underlying `CubeQueryStep`s, each producing its own `ICuboid`, the engine must merge those N cuboids into a single deduplicated stream of `SliceAndMeasures` (one entry per distinct slice, carrying a value from each underlying). This is conceptually the same operation as an **outer join** in SQL or a **merge** in dataframe libraries — but Adhoc implements it in-process, over its own column types, rather than delegating to a DB.

### Why ordering matters

External databases (DuckDB, PostgreSQL, …) can efficiently produce `GROUP BY` results in the grouping-key order (`ORDER BY` is often free when the engine already sorts for `GROUP BY`). When the table wrapper exposes a `TabularRecordStream` that arrives **slice-sorted**, the downstream engine can exploit this ordering in two ways:

1. **Sorted-merge join** (`MergedSlicesIteratorNavigableElseHash`) — merges N sorted cuboid iterators in O(N × M) total comparisons (M = total slices) with no extra memory beyond a priority queue of size N. This is the production fast path for `Combinator`s whose underlyings share the same `GROUP BY`.

2. **Append-last insert** (`MultitypeNavigableColumn`) — sorted incoming slices always hit the append-last fast path (O(1) per insertion, no binary search, no hash computation). The resulting column is itself sorted, enabling the next step to also benefit from merge-join.

### When ordering breaks

Several measure types legitimately break the input ordering:

|        Measure type         |                                                                                                                           Why ordering breaks                                                                                                                            |                                                                   Consequence                                                                   |
|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| **Sparse slice sets**       | Different underlyings may contribute different subsets of slices. An underlying that has no value for a given slice does not appear in its sorted iterator, so the merger must fall back to point-lookup via `onValue(slice, SORTED_SUB_COMPLEMENT)`.                    | The `SORTED_SUB_COMPLEMENT` stream (the hash-side tail of `MultitypeNavigableElseHashColumn`) handles these leftovers.                          |
| **`Partitionor`**           | Often changes the `GROUP BY` — the output grouping may differ from the input grouping, so the input-sorted order is meaningless in the output space.                                                                                                                     | Output column accumulates values via a mergeable hash or navigable-else-hash column.                                                            |
| **`Dispatchor`**            | Creates entirely new slices (e.g. dispatching a value across multiple buckets of a new column). The new slices have no inherent ordering relative to the input.                                                                                                          | Same — output is hash-accumulated.                                                                                                              |
| **`UndictionarizedColumn`** | The dictionarization assigns sequential integer indices to slices in insertion order. The index ordering matches slice ordering only for a leading prefix; once an out-of-order slice arrives, the correspondence breaks (see `AAggregatingColumns.sortedPrefixLength`). | `UndictionarizedColumn.stream(SORTED_SUB)` exposes only the genuinely-sorted prefix; `stream(SORTED_SUB_COMPLEMENT)` covers the unordered tail. |

### The sorted-leg / complement split

To handle the mixed case uniformly, every `IMultitypeColumnFastGet` supports `stream(StreamStrategy)`:

- `SORTED_SUB` — the slice-sorted sub-stream. For `MultitypeNavigableColumn` this is everything; for `MultitypeNavigableElseHashColumn` it is the navigable side; for `UndictionarizedColumn` it is the first `sortedPrefixLength` indices.
- `SORTED_SUB_COMPLEMENT` — the unordered remainder. For navigable columns this is empty; for navigable-else-hash it is the hash side; for `UndictionarizedColumn` it is indices `≥ sortedPrefixLength`.
- `ALL` — both legs concatenated.

The join helper (`UnderlyingQueryStepHelpersNavigableElseHash.distinctSlices`) first merge-joins the sorted legs across cuboids, then iterates each cuboid's complement, deduplicating against already-seen slices. This two-phase approach maximises the benefit of sorted data while still handling arbitrary unordered slices.

### Comparison with external systems

|     System      |                                           Join model                                            |                                                                                        Sorted-input optimisation                                                                                         |
|-----------------|-------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SQL engines     | Hash join, merge join, nested-loop join; planner picks based on statistics and indexes.         | Merge join is chosen when both sides have a compatible sort order (index scan, explicit `ORDER BY`).                                                                                                     |
| Pandas / Polars | `merge()` / `join()` on columns; Polars can exploit sorted flags to use merge join.             | Polars' `is_sorted` column flag enables O(N) merge join instead of O(N log N) sort-then-merge.                                                                                                           |
| DuckDB          | Adaptive join: hash join by default, merge join when profitable.                                | Pipelines may preserve sort order through operators; merge join is used when inputs are pre-sorted.                                                                                                      |
| **Adhoc**       | Two-phase merge: sorted legs via priority-queue merge, then hash-side complement via set dedup. | Relies on the DB producing a sorted `GROUP BY` result (common for single-table queries). Tracks the sorted prefix length via `AAggregatingColumns.sortedPrefixLength` so partial ordering is never lost. |

### References

- [Sort-merge join](https://en.wikipedia.org/wiki/Sort-merge_join) — the classical algorithm Adhoc's sorted-leg merge is based on.
- [Hash join](https://en.wikipedia.org/wiki/Hash_join) — the alternative when inputs are unsorted; Adhoc's hash-side complement lookup is a point-lookup variant.
- DuckDB's [join ordering paper](https://duckdb.org/2022/05/27/leanstore.html) — background on adaptive join selection in a modern OLAP engine.
- Polars' [sorted flag](https://docs.pola.rs/user-guide/concepts/data-types-and-structures/#sorted-flag) — the same idea as `sortedPrefixLength` but at the column metadata level, not per-wrapper.
- See also [optimisations.md](optimisations.md) § "Values / Aggregates" for the column types involved, and [partitionor.md](partitionor.md) / the `Dispatchor` section for the measures that break ordering.

