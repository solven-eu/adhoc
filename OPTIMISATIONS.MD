# Concurrency

## Immutability

Most data-structures are immutable to enable easy concurrency.
- AdhocMap

## Thread pools

`AdhocUnsafe` exposes three pools, each tuned for a different workload:

| Field | Type | Purpose |
|---|---|---|
| `adhocMixedPool` | Virtual-thread executor (`newThreadPerTaskExecutor`) | Mixed / IO-bound work. Virtual threads are cheap and blocking-friendly, so no separate IO executor is needed. |
| `adhocCpuPool` | `ForkJoinPool` (sized to `parallelism`) | CPU-bound work. Tasks submitted from a FJP worker that call `parallelStream()` fork into this pool rather than the JVM common pool, giving predictable naming and sizing. |
| `maintenancePool` | Cached daemon thread pool | Background maintenance (e.g. `CacheBuilder.refreshAfterWrite`). Daemon so it never prevents JVM shutdown. |

All three can be replaced before the first query is executed. `AdhocUnsafe.resetAll()` recreates them to a clean default state (useful in tests).

## DAG (Direct-Acyclic-Graph)

A bunch of operations are abstracted as DAG. The concurrent execution of the steps of the DAG is managed by `DagCompletableExecutor` with `CompletableFuture` formalism.

~We used to rely on `DagRecursiveAction implements RecursiveAction`, but it led to StackOverFlow and thread-starving~

The main DAGs are:
- Induced `tableQuerySteps`: given the table result `SELECT x1,2 GROUP BY y1,y2 WHERE z1 OR z2`, we can infer `SELECT x1 GROUP BY y1 WHERE z1`
- Measures `cubeQuerySteps`: given the underlying `cubeQueryStep`, we can evaluate the derived measures. Some step may be shared by multiple measures.

The study the ability to make a single DAG for the whole query.

# Slices/Maps

In Adhoc, we consider slices (which are similar to Maps expressing coordinates along columns). These slices are:
- numerous, as we consider many slices per CubeQueryStep
- often based on the same `.keySet()`, similarly to a `table`.
  - a `CubeQueryStep` always refers to the same columns
  - a `Combinator` refers to the same columns as its underlyings `CubeQuerySteps`
  - the inducing phase of `ATableQueryOptimizer` generates a `table` from  a `table`, by removing some columns (and applying some filter).

Hence, instead of relying on a `HashMap`, we rely on `AdhocMap` which is specialized for recurrent `.keySet()`.

## ILikeList gives `.getKey(int index)` in O(1)

The actual `.keySet()` is based over:
- `NavigableSetLikeList` for `NavigableMap`
- `SequencedSetLikeList` for `Map`, which is based over a `SequencedSetLikeList` and a re-ordering (as an `int[]`).

Both classes implements `ILikeList`, which enables processing the `.keySet()` as a `List` for:
- faster iterations

## Perfect-Hashing gives `.indexOf(String key)` in O(1)

To implement `IAdhocMap.get(String key)`, we need a fast `NavigableSetLikeList.indexOf(String key)`.

- This could be achieved with a `HashMap` from `String key` to `int index`.
- We observe better performance (x2 CPU) with a perfect-hashing (see `SimplePerfectHash`).

`SimplePerfectHash` uses power-of-two masks (bitwise AND instead of modulo) with a shift parameter to resolve collisions without growing the table. Construction tries increasing table sizes until a collision-free assignment is found.

State-of-the-art references:
- [GNU gperf](https://www.gnu.org/software/gperf/) — classic compile-time perfect hash for static C/C++ key sets.
- [sux4j](https://github.com/vigna/sux4j) (Sebastiano Vigna) — minimal perfect hash functions (MPHF) achieving ~2.24 bits/key; the academic gold standard for space-optimal static MPHFs.
- [CHD algorithm](http://cmph.sourceforge.net/chd.html) (Compress, Hash, Displace) — near-optimal MPHF construction in O(n) expected time; also implemented in [CMPH](http://cmph.sourceforge.net/).
- [PTHash](https://github.com/jermp/pthash) — fast and space-efficient MPHF with configurable space/time trade-off; well-suited for large static key sets.
- [RecordHash / BDZ / BPZ](http://cmph.sourceforge.net/bdz.html) — MPHF variants also in CMPH, useful for understanding the space lower bounds.

## Columnar storage

Each `IAdhocMap` is similar to a `HashMap`. However:
- Given we encounter many of them with the same `.keySet()`
- Given we often encounter the same values (i.e. coordinates along a column)
- We like storing the underlying data into a column-store.
- This is achieved by `ColumnarSliceFactory`, where each `IAdhocMap` refers an `int` to a `IAppendableTable`.
  - `ThreadLocalAppendableTable` enables one page per-thread.
  - `IAppendableColumnFactory` enables one `IAppendableColumn` per key
  - `IFreezingStrategy` enables encoding/compression per-block, once a page is full.

### Per-query isolation — no cross-query sharing

The columnar storage (dictionaries, compressed column pages) is **scoped to a single query**. `ISliceFactoryFactory.makeFactory(queryOptions)` creates a fresh `ISliceFactory` for every query; two independent concurrent queries never share any dictionary or column data. This is a deliberate design choice:

- **No memory leak**: once a query finishes and its result is consumed, the factory (and all its backing arrays) can be GC'd without any cross-query reference keeping it alive.
- **No cross-query lock contention**: each query's columns are private, so appending values needs no coordination with other queries.
- **Simpler lifecycle**: the factory does not need invalidation logic; it is simply abandoned after the query.

## Encodings

Adhoc enables encoding/compression, through `IFreezingStrategy`:
- Dictionarization (i.e. mapping from a `List<Object>` into an `int[]` and a `Map<Integer, Object>`).
- Primitive columns (i.e. mapping from a `List<Object>` into a `long[]` if all objects are longs).
- Binary packing (i.e. packing multiple `int`s into a `long` if input integers are known to have a small range (e.g. from 0 to 32))
- String encoding with `FSST`. Expect typically from `75%` to `25%` reduction in RAM usage.

# Values / Aggregates

`CubeQueryStep` are typically a `Map` from a `slice`/`map` to a value/aggregate. Given aggregate can be any `Object`, but it is often a `long` or a `double` (for sum aggregation).

We rely on data-structure enabling mapping from a slice to a `long`/`double`/`Object`:
- `MultitypeHashColumn` is a hash-based append-only structures, efficiently storing inputs of diverse types.
- `MultitypeHashMergeableColumn` extends `MultitypeHashColumn` by enabling merging into existing slices.
- `MultitypeNavigableElseHashColumn` is a tree-based append-only structures, efficiently storing inputs of diverse types.
- `MultitypeNavigableElseHashMergeableColumn` extends `MultitypeNavigableElseHashColumn` by enabling merging into existing slices.

Tree-based structures are useful as some computations needs to iterate along multiple structures at the same time (e.g. a `CubeQueryStep` iterates along its underlyings on a per-slice basis).

# Cache

There is multiple caches in Adhoc. They can be disabled on a query basis with `StandardQueryOptions.NO_CACHE`.

## Static caches

- `AbstractAdhocMap.CACHE_RETAINEDKEYS` enable computing once and for all some basic structures given a `GROUP BY`.

These caches:
- can be cleared with `AdhocUnsafeMap.clearCaches()`.
- does not implement `IHasCache`.

## Structural caches

- `ASliceFactory.listToKeyset` will save a `SequencedSetLikeList` per `GROUP BY`.

These caches:
- does implement `IHasCache`.

## Cross-queries caches

- `JooqTableWrapper.fieldsCache` is a cache for metadata from the table (e.g. columns metadata).
- `PivotableAsynchronousQueriesManager.queryIdToView` holds query results of asynchronous queries.
- `IQueryStepCache` enables caching of `cubeQueryStep` results from one query to another. It is typically an LRU cache. It is a property of `StandardQueryPreparator`.
- `CachingTableWrapper` is an `ITableWrapper` decorator enabling caching. It is deprecated as it seems less relevant and more difficult to tweak than a proper `IQueryStepCache`. 

These caches:
- does implement `IHasCache`.

### CachingTableWrapper

This may not lead to expected performances as reading from the cache leads to additional effort (e.g. merging a `ISliceToValue` from the table for one measure, and another from the cache for another measure requires some JOIN operation (e.g. aligning the slices from the 2 distinct `ISliceToValue`), which may not happen if both measures was queried in the same `tableQuery`).

## Intra-query caches

- `CubeQueryStep.getCache()` is a `ConcurrentMap` you can freely read-write for caching elements of business-logic at a given step. `ManyToMany1DDecomposition` is  usage example.
- `CubeQueryStep.getTransverseCache()` is another `ConcurrentMap` which is shared by all `CubeQuerySteps`. It enables two different measures to share some cached value.
- `FilterOptimizerWithCache` enables faster `ISliceFilter` operations in the context of a single query.

These caches:
- may not implement `IHasCache`.

# Filters - Boolean Arithmetic

`ISliceFilter` implements boolean arithmetic, by relying on 4 operators:
- AND: a standard AND operator. If empty, it means `matchAll`.
- OR: a standard OR operator. If empty, it means `matchNone`.
- NOT: a standard NOT operator.
- ColumnFilter: enables an `IValueMatcher` over a column

Given we build DAG of `CubeQueryStep`s, and `CubeQueryStep.filter` is part of the `hashCode/equals` contract, we need `ISliceFilter` to be consistent regarding `hashCode/equals`. This is a very subtle requirement, as many different boolean expressions can be equivalent (e.g. `!(true) == false` or `a==a1|a==a2 == a=in=(a1,a2)`).

`IFilterOptimizer` enables simplifying `ISliceFilter`/boolean expressions while ensuring 2 equivalent `ISliceFilter` are actually equal. This is a complex component as it should be:
- fast, as we tend to receive complex boolean expression, especially given deep tree of measures (as filters are modified by `Shiftor` or other transformators).
- consistent, as the engine may itself split filters (e.g. when building a DAG of induced tableResults).
- optimizing, to help producing simpler queries to the `ITableWrapper`

Current `ISliceFilter` optimizations includes:
- `a==a1|a:like:a%` is automatically turned into `a:like:a%`
- a cost function, to decide when to prefer `!a&!b&!c` over `!(a|b|c)`.

## IValueMatcher vs ISliceFilter

A `ISliceFilter` represents a slice over a cube: simplest filters are essentially a `Map` (meaning an `AND`) from a `column` to an `IValueMatcher`.
A `IValueMatcher` represents a slice within given column.

Current `IValueMatcher` optimizations includes:
- `==a1|like:a%` is automatically turned into `like:a%`

# `ITableQuery` optimizations - Grouping `Aggregators` into an optimal set of `SQL`s

Given a set of `CubeQueryStep` referring an `Aggregator`, these may be grouped together to minimize the number of queries submitted to `ITableWrapper`.

For instance:
- `SUM(a) WHERE f1 AND f2` and `SUM(B) WHERE f1` may be grouped into `SUM(a) FILTER f2, SUM(b) WHERE f1`

It is also possible to change the way queries are grouped together (typically on a per-`GROUP BY` basis) in order to execute less queries (though these queries would query irrelevant information):

```java
CubeWrapper cube = CubeWrapper.builder()
    .name(table.getName())
    .table(table)
    .forest(forest)
    .engine(CubeQueryEngine.builder()
            .tableQueryEngine(TableQueryEngine.builder().optimizerFactory(new ITableQueryOptimizerFactory() {

                @Override
                public ITableQueryOptimizer makeOptimizer(AdhocFactories factories, IHasQueryOptions hasOptions) {
                    if (hasOptions.getOptions().contains(InternalQueryOptions.DISABLE_AGGREGATOR_INDUCTION)) {
                        return new TableQueryOptimizerNone(factories);
                    } else {
                        return new TableQueryOptimizerSinglePerAggregator(factories);
                    }
                }
            }).build())
            .build())
    .build();
```

`TableQueryOptimizerSinglePerAggregator` will merge all `GROUP BY` in a union of columns, and filters into a `OR`. Hence, in a single query, 
it will be able to fetch al lthe necessary information.

For instance:
- Given `SUM(a1) GROUP BY b1 WHERE c1`
- and `SUM(a2) GROUP BY b2 WHERE c2`
- it will query `SUM(a1), SUM(a2) GROUP BY b1, b2 WHERE c1 OR c2`
- If `GROUP BY b1, b2` is dense, and returns `p * q` cells where `p` is `GROUP BY b1` cardinality and `q` is `GROUP BY b2` cardinality, it leads to a query sensibly more complex than the 2 simpler ones.
