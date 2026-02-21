# Concurrency

## Immutability

Most data-structures are immutable to enable easy concurrency.
- AdhocMap

## ForkJoinPool

By default, `adhoc` relies on a custom ForkJoinPool. It can be customized in `AdhocUnsafe.adhocCommonPool`.

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

## Columnar storage

Each `IAdhocMap` is similar to a `HashMap`. However:
- Given we encounter many of them with the same `.keySet()`
- Given we often encounter the same values (i.e. coordinates along a column)
- We like storing the underlying data into a column-store.
- This is achieved by `ColumnarSliceFactory`, where each `IAdhocMap` refers an `int` to a `IAppendableTable`.
  - `ThreadLocalAppendableTable` enables one page per-thread.
  - `IAppendableColumnFactory` enables one `IAppendableColumn` per key
  - `IFreezingStrategy` enables encoding/compression per-block, once a page is full.

# Values / Aggregates

`CubeQueryStep` are typically a `Map` from a `slice`/`map` to a value/aggregate. Given aggregate can be any `Object`, but it is often a `long` or a `double` (for sum aggregation).

We rely on data-structure enabling mapping from a slice to a `long`/`double`/`Object`:
- `MultitypeHashColumn` is a hash-based append-only structures, efficiently storing inputs of diverse types.
- `MultitypeHashMergeableColumn` extends `MultitypeHashColumn` by enabling merging into existing slices.
- `MultitypeNavigableElseHashColumn` is a tree-based append-only structures, efficiently storing inputs of diverse types.
- `MultitypeNavigableElseHashMergeableColumn` extends `MultitypeNavigableElseHashColumn` by enabling merging into existing slices.

Tree-based structures are useful as