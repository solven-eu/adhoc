# Slice and IAdhocMap

## ISlice — the domain view

An `ISlice` represents a single row in a query result: the set of column-value pairs that identify
one cell in a pivot table. If a query groups by `desk` and `ccy`, a typical slice is
`{desk=Equity, ccy=USD}`.

The interface is intentionally domain-friendly:

```java
// Read a coordinate value
Object desk = slice.getRaw("desk");

// Iterate all coordinates
slice.forEachGroupBy((column, value) -> ...);

// Project to a subset of columns (produces a new, smaller slice)
ISlice projected = slice.retainAll(NavigableSet.of("desk"));

// Convert to a filter — useful when passing a slice as a constraint downstream
ISliceFilter filter = slice.asFilter();   // AND( desk=Equity, ccy=USD )

// The grand-total slice has no coordinates
boolean isGrandTotal = slice.isEmpty();
```

`ISlice` is also `Comparable`, so slices can be used as sorted-map keys directly.

### ISliceWithStep

When a slice is passed to a measure's combination logic (e.g. `ICombination.combine(...)`), it
arrives as `ISliceWithStep` — a thin extension that adds the `CubeQueryStep` context. This lets the
combination function know *which* measure and *which* groupBy produced this slice, without the
domain code needing to be aware of the query engine internals.

---

## IAdhocMap — the technical contract

Every `ISlice` is backed by an `IAdhocMap`, accessible via `slice.asAdhocMap()`. The two views are
interchangeable — `IAdhocMap.asSlice()` wraps it back — but they serve different audiences:

|                 |                   `ISlice`                   |                   `IAdhocMap`                    |
|-----------------|----------------------------------------------|--------------------------------------------------|
| **Audience**    | Measure authors, filter logic                | Engine internals, optimisation code              |
| **Abstraction** | A named row in a pivot table                 | A specialised `SequencedMap<String, Object>`     |
| **Key methods** | `getRaw`, `asFilter`, `retainAll`, `isEmpty` | `get`, `sequencedKeySet`, `retainAll`, `asSlice` |

`IAdhocMap` extends `SequencedMap<String, Object>` and adds three strong contracts:

### 1. Immutability

Every `IAdhocMap` is fully immutable. Mutation methods (`put`, `remove`, `clear`) throw
`UnsupportedAsImmutableException`. Because the content can never change, the hash code is computed
once and cached — exactly like `String.hashCode()`. The map is therefore safe to use as a
`HashMap` key, to share across threads without synchronisation, and to cache without defensive
copying.

See [Immutability](immutability.md) for the broader design rationale.

### 2. Stable iteration order (SequencedMap)

Keys are always iterated in insertion order. This makes result sets reproducible and allows the
engine to compare two maps position-by-position rather than by key lookup, enabling several
O(n)-instead-of-O(n log n) comparisons.

### 3. Perfect-hash key lookup

The key set is backed by `SimplePerfectHash`: for a fixed set of column names, a minimal perfect
hash function maps every key to a unique array slot in one multiply-and-mask operation. There are no
collision chains. A `get(column)` call is a single array access after one arithmetic step.

This matters inside the hot path of the query engine, where the same columns are looked up millions
of times per query across thousands of slices.

---

## retainAll and cached projections

Both `ISlice` and `IAdhocMap` expose `retainAll(columns)`, which returns a new map/slice keeping
only the specified columns. This is called constantly during groupBy projection (e.g. projecting a
`{desk, ccy, tenor}` slice down to `{desk}` for the parent node).

The engine caches the mapping from `(full keyset, retained columns)` to the resulting `RetainedKeySet`
in a static `ConcurrentHashMap`. The first projection for a given pair pays the construction cost;
every subsequent call for the same pair is a direct array lookup. In practice, the same handful of
projections recur across the entire query, so the cache is almost always warm.

---

## Concrete implementations

|        Class         |          Backing storage           |                                        Use                                         |
|----------------------|------------------------------------|------------------------------------------------------------------------------------|
| `MapOverLists`       | `List<?>` of values                | Default row-oriented construction                                                  |
| `MapOverIntFunction` | `IntFunction<Object>`              | Dictionary/columnar encoding; lazy value lookup                                    |
| `MaskedAdhocMap`     | Another `IAdhocMap` + a mask `Map` | Adds temporary columns (e.g. from `IColumnGenerator`) without copying the original |

`MaskedAdhocMap` is particularly useful for composite tables and many-to-many scenarios where a
handful of synthetic columns need to be injected on top of an existing slice without allocating a
full new map.

---

## Construction

Slices are normally built by the engine; measure authors receive them already constructed.
When writing tests or custom table wrappers, use:

```java
// From a plain Map
ISliceFactory factory = ...; // obtained from the cube/engine context
IAdhocMap map = AdhocMapHelpers.fromMap(factory, Map.of("desk", "Equity", "ccy", "USD"));
ISlice slice = map.asSlice();

// Grand-total slice (empty coordinates)
ISlice grandTotal = SliceHelpers.grandTotal();
```

---

## Further reading

- [Immutability](immutability.md) — why immutable data structures improve correctness beyond thread safety
- [Optimisations](optimisations.md) — perfect hashing, columnar encoding, and filter arithmetic
- [Concepts](concepts.md) — where slices fit in the overall query flow

