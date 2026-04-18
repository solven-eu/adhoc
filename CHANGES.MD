This document is intended for Adhoc developers and users.

We adhere to the [keepachangelog](https://keepachangelog.com/en/1.0.0/) format


## [0.0.19]

### Added
- `StandardQueryOptions.PARTITIONED`: new query option that splits the output column into N hash partitions (one per available core) during aggregation. Each partition is an independent `PartitionedMergeableColumn`, reducing per-partition map size and enabling future parallel reduction across cores.
- `IConsumingStream.forEachPartitioned`: partition-aware terminal operation that routes elements to dedicated per-partition consumer threads, eliminating write contention. Used by `TabularRecordStreamReducer` when the `PARTITIONED` option is active.
- `LastLookupCache1`: new length-1 specialization of `LastLookupCache` for hot-path callers that only need a single reference key. Avoids the `Object[]` varargs allocation and list wrapping on the slow path.
- `LastLookupCache1Volatile`: alternative length-1 cache using a single `volatile` shared slot instead of a `ThreadLocal`. Eliminates the `ThreadLocal.get()` map lookup at the cost of cross-thread contention on the slot. A JMH benchmark in `jmh/.../BenchmarkLastLookupCache` compares both variants against a plain `ConcurrentHashMap` baseline.
- `SequencedSetUnsafe.internKeyset(Collection)`: new static utility that interns a column keyset into a `SequencedSetLikeList`, backed by a shared two-layer cache.
- `MultitypeNavigableIntColumn`: int-specialized companion to `MultitypeNavigableColumn` (and complement to `MultitypeHashIntColumn`). Stores ascending int keys in a primitive `IntArrayList`, exposing `append(int)` / `onValue(int)` / `appendIfOptimal(int)` without `Integer` boxing. Designed as the output column of dictionarizing aggregation layers (e.g. `AggregatingColumnsDistinct`) where indices are emitted in monotonically-ascending order, so the `onAppendLast` fast path is the common case.

### Fixed
- `CompositeCubesTableWrapper`: a filter or groupBy on a column that is declared as `generated` / `calculated` by at least one sub-cube (e.g. a scenario hierarchy exposed by an `IDecomposition` inside a Dispatchor) is now auto-suppressed for sub-cubes that do NOT declare the column, instead of being translated to `matchNone` (filter) or a cube-name fallback coordinate (groupBy). A new helper `computeCrossCubeCalculatedColumns()` returns the set of cross-cube calculated columns; `filterForColumns` short-circuits to `MATCH_ALL` and the missing-column mask in `openStream` collapses to `IColumnGenerator.COORDINATE_GENERATED` for them. Fixes composite-cube counts when a `count(*)`-style aggregator is queried alongside a scenario hierarchy known only to some sub-cubes.
- `FilterHelpers.filterComparator()`: now correctly returns 0 for semantically equivalent filters with different operand order (e.g., `AND(a,b)` vs `AND(b,a)`). Type ranking: AND > OR > NOT > COLUMN.
- `IConsumingStream.iterator(ExecutorService)`: now calls `endOfData()` when the poison marker is read. Previously, the iterator returned `null` without transitioning Guava's `AbstractIterator` to `DONE`, causing any subsequent `hasNext()` call to block indefinitely on `queue.take()`.

### Changed
- `CubeWrapper.getColumns()`: aliases whose underlying column cannot be resolved on this cube are now discarded with a `WARN` log, rather than silently registered as dangling `Object.class` columns tagged `alias`. This prevents a `ColumnsManager` shared across several cubes (where some aliases are only relevant to a subset of cubes) from polluting the schema reported by each cube. The `log.debug` line also had a broken `String.format`-style `%s` placeholder, now corrected to the SLF4J `{}` form.
- `AMultitypeNavigableColumn` (new abstract parent): factored the shared write/append/scan/stream/lookup/lock/compact logic out of `MultitypeNavigableColumn` and `MultitypeNavigableIntColumn` into a common base parameterised on `T extends Comparable<T>`. Subclasses now own only the key-storage hooks (`keysSize`, `keysGet`, `keysAppendLast`, `keysInsertAt`, `keysRemoveAt`, `keysBinarySearch`, `keysEnsureCapacity`, `keysCompact`) and the optional Bloom presence pre-screen. The protected helper formerly named `merge(int)` is now `mergeAtIndex(int)` to avoid colliding with the public `IMultitypeMergeableColumn.merge(T)` signature on the int specialization. Net effect: the two navigable subclasses drop ~720 LOC of duplicated logic, the int specialization keeps its primitive `IntArrayList` storage, and the only new boxing happens at the abstract-method boundary on the slow paths (binary search / random insertion / scan).
- `AMultitypeHashColumn` (new abstract parent): identical extraction for the hash family. The new base owns the type-dispatch logic for `containsKey`, `append`, `unsafePut`, `merge`, `onValue`, `scan`, `stream`, `keyStream`, `unsafePurge` and `compact`, while subclasses provide ~24 thin abstract hooks per type-lane (size / containsKey / get / put / remove / forEach / ensureCapacity / compact). `MultitypeHashColumn` and `MultitypeHashIntColumn` shrink to just the fastutil `Object2X` / `Int2X` map declarations and one-liner forwarders. Storage stays primitive in the int variant; the per-call boxing is short-lived garbage that doesn't change the steady-state heap.
- `MultitypeNavigableIntMergeableColumn`: new int-specialized companion to `MultitypeNavigableMergeableColumn`, available for callers that want an unboxed `merge(int)` fast path. **Not** yet wired into `AggregatingColumns.makePreColumn()` because the slice-sortedness tracking that the existing `MultitypeNavigableElseHashMergeableColumn` provides for free (via its in-order/out-of-order leg dispatch) is lost without a hash-side fallback; moving that tracking into `AAggregatingColumns` is a separate task.
- `UndictionarizedColumn.column`: tightened field type from `IMultitypeColumnFastGet<Integer>` to `IMultitypeIntColumnFastGet`, so hot-path `onValue(int)` / `onValue(int, StreamStrategy)` lookups never pay `Integer.valueOf` allocation. `UndictionarizedColumn` now also implements `ICompactable`, forwarding to the inner column.
- `UndictionarizedColumn.stream(SORTED_SUB)` / `stream(SORTED_SUB_COMPLEMENT)`: switched from positional prefix/suffix (`column.limit(sortedLength)` / `column.skip(sortedLength)`) to `sortedLeg`-predicate filtering. Positional splits were only correct when the inner column's natural iteration order aligned with the slice-sorted prefix; after the refactor below, the inner column always iterates by index which may not coincide, so the filter is the authoritative path.
- `AggregatingColumnsDistinct.makePreColumn()`: now instantiates `MultitypeNavigableIntColumn` directly instead of going through `IColumnFactory.makeColumn(...)`. Indices arrive in monotonically-ascending order, so the append-last fast path is hit on every write and no boxing-through-`Integer` happens anywhere in the aggregation or close-column hot paths. `aggregatorToAggregates` is now typed `Map<String, IMultitypeIntColumnFastGet>`; the previously-unused `factories` field has been removed from the builder.
- `AggregatingColumns.undictionarizeColumn`: copies the generic `IMultitypeColumnFastGet<Integer>` produced by the non-distinct aggregation path into a fresh `MultitypeNavigableIntColumn` via `asNavigableInt(...)` before wrapping in `UndictionarizedColumn`. Columns that are already int-specialized (the `AggregatingColumnsDistinct` path) short-circuit to identity.
- `IMultitypeIntColumnFastGet.purgeAggregationCarriers()`: covariant narrowing — the interface now declares the return type as `IMultitypeIntColumnFastGet` so callers (e.g. `UndictionarizedColumn.purgeAggregationCarriers`) get the int-specialized type back without an `instanceof` re-check. `MultitypeNavigableIntColumn.purgeAggregationCarriers()` now returns the concrete class directly.
- `AdhocUnsafe.adhocCommonPool`: replaced `ForkJoinPool`-based work-stealing pool with `Executors.newVirtualThreadPerTaskExecutor()`. Virtual Threads are blocking-friendly, eliminating the need for a separate I/O thread pool.
- `QueryPod`: removed `dbExecutorService` field — a single `executorService` (Virtual Thread executor for concurrent queries) now covers both DAG walking and table I/O.
- `StandardQueryPreparator`: removed `concurrentDBExecutorService`, `nonConcurrentDBExecutorService`, `getDBExecutorService`, and the `isDuckDB` check; all queries use the same `executorService`.
- `AdhocTableUnsafe.setDbParallelism`: now a no-op kept for source compatibility; DB concurrency is controlled by `AdhocDuckDBUnsafe.duckDBSemaphore` instead of a thread pool.
- `AdhocDuckDBUnsafe`: replaced `adhocDuckDBPool` (fixed-size thread pool) with `duckDBSemaphore` (`Semaphore(2)` by default). Limits concurrent DuckDB queries without blocking Virtual Threads.
- `ITableWrapper`: new `querySemaphore()` default method returning `null` (no limit). Implementations can return a `Semaphore` to cap concurrent queries.
- `JooqTableWrapperParameters`: new optional `querySemaphore` field. `JooqTableWrapper` implements `querySemaphore()` from this parameter and applies it lazily in `streamSlices()`.
- `DuckDBHelper.parametersBuilder(IDSLSupplier)`: new factory method returning a `JooqTableWrapperParameters` builder pre-wired with `AdhocDuckDBUnsafe.duckDBSemaphore`.
- `TableQueryEngineBootstrapped` → `TableQueryEngine`; `TableQueryEngine` → `TableQueryEngineFactory` (rename).
- `ICombination`: signature changed from returning an `IValueProvider` to accepting an `IValueReceiver` as input, enabling push-based value delivery and avoiding intermediate allocations.
- `LastLookupCache`: single generic parameter `<V>` (dropped the custom map-key type parameter); a required `keyLength` is now passed to the constructor; `slowComputeIfAbsent` takes only a `Supplier<V>` and uses `Arrays.asList(refKeys)` as the delegate map key.
- `ISliceFactory.internKeyset(Collection)`: removed from the interface — replaced by `SequencedSetUnsafe.internKeyset(Collection)` (static). Callers that previously called `sliceFactory.internKeyset(keys)` should now call `SequencedSetUnsafe.internKeyset(keys)`.

## [0.0.18]

### Fixed
- `AddSharedNodes`: fixed missing sub-sharing when a newly inserted shared node's induced children contain further sharing opportunities; new node is now processed eagerly (via recursive `addSharedNode`) before control returns to the caller. Also replaced the `TopologicalOrderIterator`+dirty-requeue loop with a single linear pass over a pre-snapshotted `ImmutableList` (JGraphT contract: iterator must not be used while the graph is mutated).
- `TableWrapperHelpers.closeAll`: when closing multiple `ITabularRecordStream`s, exceptions from one stream no longer prevent the others from being closed; subsequent exceptions are attached as suppressed causes on the first.
- `InduceByAdhoc.splitInducedDag`: fixed NPE when input steps have non-contiguous groupBy cardinalities (e.g. sizes 1 and 3 with no size-2 step) — iterating through missing cardinalities now yields an empty list instead of `null`.

### Changed
- `InduceByAdhocComplete`: concurrent execution of `buildEdgesForContextGroup` is now supported — when `StandardQueryOptions.CONCURRENT` is active, each measure×context group is processed in an isolated local DAG on the injected `ListeningExecutorService`, then all local DAGs are merged before the transitive-reduction step. The default executor is `MoreExecutors.newDirectExecutorService()` (sequential, as before). Inject a thread-pool via `InduceByAdhocComplete.builder().executorService(...).build()`.
- `InduceByAdhocComplete.splitInducedAsDag`: replaced O(n²) pairwise scan with three pre-computed indices — (1) grouping by measure+context to avoid cross-measure evaluation, (2) a groupBy containment index (O(g²) for g distinct groupBys), and (3) a filter strictness index (O(f²) for f distinct filters). Only `makeLeftoverFilter` is still checked per step pair, reached only when both indices agree.

### Added
- `IFilterFactories`: new interface coupling `IFilterStripperFactory` + `IFilterOptimizerFactory` at configuration time, with a `makeQueryBundle()` default method that produces a per-query `IFilterQueryBundle` (contextualised stripper + cached optimizer). `IAdhocFactories` now extends `IFilterFactories`.
- `IFilterQueryBundle`: new interface grouping the query-scoped `IFilterStripperFactory` and `IFilterOptimizer` that are created once per query and shared across all sub-strategies (`ITableStepsSplitterFactory`, `IMergeInducersFactory`, `IAddSharedNodesFactory`). Inner factory methods now take a single `IFilterQueryBundle` parameter instead of the previous two-argument pair.
- `AdhocUnsafe.adhocDbPool`: dedicated thread pool (default 2 threads, unbounded queue) for external database queries (e.g. DuckDB), preventing I/O-bound DB calls from starving the CPU-bound common pool. Configurable via `-Dadhoc.dbParallelism=N`. `StandardQueryPreparator` exposes a `dbExecutorService` field (defaulting to `adhocDbPool`), enabling per-`CubeWrapper` configuration; `QueryPod` propagates it to `TableQueryEngineBootstrapped`, which submits concurrent `TableQueryV4` queries to it instead of using parallel streams on the CPU pool.
- `FilterOptimizer`: the AND optimizer now clusters operands by independent column sets before optimising. Operands whose column sets are fully disjoint are optimised independently and then AND-ed, enabling the distributive-law folding `(A&B)|(A&C) → A&(B|C)` to fire correctly. `FilterHelpers.clusterFilters` exposes the clustering logic.
- `ICubeQueryStep`: new interface grouping the contracts of `CubeQueryStep` (`IWhereGroupByQuery`, `IHasMeasure`, `IHasCustomMarker`, `IHasQueryOptions`, `IHasCache`); `CubeQueryStep` now implements it, enabling polymorphic handling of step types.
- `TableQueryStep`: strongly-typed record wrapping a `CubeQueryStep` whose measure is guaranteed to be an `Aggregator`; replaces the raw `(Aggregator) step.getMeasure()` cast in the table-query execution path. `StepAndFilteredAggregator.step` now carries a `TableQueryStep` instead of a plain `CubeQueryStep`.
- `FlatAndFilter`: compact `IAndFilter` backed by `ImmutableMap<String, IValueMatcher>` — operands are guaranteed flat per-column conditions with no recursive `ISliceFilter` children. `ISlice.asFilter()` now returns a `FlatAndFilter`, and `FilterHelpers.emitAndOperands` / `FilterHelpers.asMap` have dedicated fast-paths for it. `SliceFilterSerializer` serialises it as the canonical `"type":"and"` form.
- `scripts/check-coverage-sonar.pl`: offline Perl script that queries the SonarCloud API (no local build needed) and lists production files below a configurable coverage threshold (default 20 %). Run with `perl scripts/check-coverage-sonar.pl [--threshold N]`.
- `scripts/check-coverage-low.pl`: local Perl script that parses JaCoCo XML reports and reports the same threshold violations; runs automatically in CI after the test step.
- `CONVENTIONS.MD` Test Coverage section: documents the 80 % instruction-coverage target and the 20 % critical floor.
- `TpchSchema`: TPC-H example schema using DuckDB's built-in `tpch` extension (`CALL dbgen(sf=0.1)`); builds a snowflake view (`lineitem → orders → customer → nation → region`, `lineitem → part`, `lineitem → supplier`) with measures for `revenue` (expression aggregation), `quantity`, `avg_discount`, `order_count`, and `customer_count`. Accompanied by `TestTableQuery_DuckDb_Tpch`.
- `TableQueryV4`: new query model that explicitly maps each `IGroupBy` to the exact `FilteredAggregator`s it needs, eliminating the cartesian-product waste of `TableQueryV3`. `streamV3()` collapses groupBys with identical aggregator sets into a single GROUPING SET and emits the rest as independent V3s (UNION ALL). `TableQueryFactory.processSteps` now builds V4 internally.
- `MeasureForestConcealer`: utility that produces a "concealed" copy of a `MeasureForest` safe for export — measure names, column names, filter column names, and tags are replaced by deterministic hashes; options are preserved unchanged by default (extensible via `concealOptions()`). `concealWithDefinition()` also returns a `ConcealingDefinition` that a trusted party can pass to `restore()` to recover the original forest.
- `MeasureForestAnalyzer`: utility that analyses a `MeasureForest` and returns (1) `columnToSpecialValues` — a map from every referenced column to the set of equality/IN-membership values in filter conditions, and (2) `tagToColumns` — a `SetMultimap` from tags to the columns referenced by measures carrying that tag.

### Changed
- Upgraded SpringBoot to `4.1.0-M2`
- Upgraded DuckDB to `1.5.0.0`
- Upgraded Arrow to `19.0.0`
- Renamed `IAdhocSlice` into `ISlice`.
- Renamed `SliceAsMap` into `Slice`.

## [0.0.17]

### Added
- Arrow IPC streaming endpoint: `POST /api/v1/cubes/query` with `Accept: application/vnd.apache.arrow.stream` returns query results as a streaming Arrow IPC response. Backed by `TabularViewArrowSerializer` (in `adhoc-experimental`) discovered via `ServiceLoader`. Supports `Long`, `Integer`, `Double`, `Boolean`, `LocalDate`, and `String` column types.
- `MavenDependencyAsMermaid`: utility that scans `pom.xml` files across a multi-module Maven project and renders a Mermaid `flowchart TD` of inter-module dependencies; compile edges use solid arrows, test-scope edges use dashed arrows, and modules are grouped into `subgraph` blocks by their aggregator parent.
- `ColumnarTabularView`: columnar alternative to `ListBasedTabularView` — stores one `List<?>` per coordinate column and one per aggregate, enabling zero-transposition Arrow serialization.
- `pivotable-mcp` module: exposes Pivotable as an MCP server (list endpoints, browse schemas, execute queries) via `PivotableMcpTools`.
- `PivotableChatHandler`: optional AI query assistant in the SPA, streaming tool calls to update the query model in real time (requires `adhoc.pivotable.chat.anthropic-api-key`).
- `DuckDBMemoryLogger`: daemon that periodically samples `duckdb_memory()` and logs results; self-terminates when the monitored connection closes.
- `TableQueryV3` enables `GROUPING SET` queries.
- `ITableQueryStepsSplitter` enables to inject a splitting policy into an `ITableQueryOptimizer`.
- `ITableStepsGrouper` is introduce to facilitate the customization of `TableQueryV3` granularity. It is the opportunity to have dedicate `TableQueryV3` on specific columns (e.g. very high cardinality ones).
- `ITableStepsSplitter` is introduce to facilitate the customization of `CubeQueryStep` which should be induced by Adhoc. It is the opportunity to have dedicate `TableQueryV3` on specific columns (e.g. very high cardinality ones).
- Introduced `AdhocCaseInsensitivityUnsafe`. Beware, CaseInsensitivity is not fully ready yet.
- Introduce `Shiftor.ShiftorBuilder.lambda(ILambdaFilterEditor)` which enables defining an `IFilterEditor` through a lambda.

### Changed
- `ISliceToValue` is renamed `ICuboid`.
- Introduce `adhoc-dataframe`, `adhoc-measures`, `adhoc-cell` maven modules.
- `ITableQueryOptimizer` implementations has been fully reworked given `TableQueryV3`
- `TableQueryOptimizer` is renamed `TableQueryFactory` as it has no more any optimizing logic.


## [0.0.16]

### Added
- Introduce `Combinator.CombinatorBuilder.lambda(ILambdaCombination)` which enables defining an `ICombination` through a lambda.
- Introduce `Aggregator.AggregatorBuilder.lambda(ILambdaAggregation)` which enables defining an `IAggregation` through a lambda.
- Introduce `adhoc-encoding` module, to remove core algorithms from Adhoc Query Engine.
- Prepare partitioning of table queries (e.g. `IQueryPartitionor`), to help concurrency in table own engine, and in Adhoc by received partitioned results.
- `DuckDBTableWrapper` enables Arrow-support for fetching query results.
- `AdhocUnsafe.getNullComparator()` for `Map` comparison. This behavior can be customized with `AdhocUnsafe.setNullComparator()`.
- Enable customization of JDBC `fetchSize` through `JooqTableWrapperParameters.statementFetchSize`. Default is `16*1024=16_384`.
- Add probe to warn if JDBC `autoCommit` is false.
- Introduce DuckDB as an option to reduce induced tableResults (see `IInducedEvaluator`).
- Introduce `ClickHouseTableWrapper` to enable Arrow connectivity.

### Changed
- Optimize `CompositeCubesTableWrapper.transcodeSliceToComposite` by computing the missing columns mask only once, and leveraging `IAdhocSlice.addColumns(Map<String, ?>)`.
- `adhoc-atoti` dependencies over ActivePivot/Atoti are not minimal (e.g. no more `activepivot-server-spring`).
- Move Checkstyle configuration so that a call to `mvn checkstyle:check` relies on the standard Adhoc configuration.
- `DSLSupplier` is renamed `IDSLSupplier`.
- `AdhocMapHelpers.immutableCoyOf(Map)` is moved to `ImmutableHelpers.copyOf(Map)`
- Introduced `testcontainers` to check Adhoc against various Databases (PostgreSQL, YugaByteDB, CLickHouse, MySQL, MariaDB, )

### Fixed
- `SequencedSetLikeList.toArray()` now returns a sequenced array. It fixes bugs around `ImmutableList.copyOf()`.
- `AbstractAdhocMap.compareTo` was not managing `NULL_HOLDER` properly.

## [0.0.15]

### Added
- Introduce `AsynchronousFreezingStrategy` which will freeze in a different thread and then update some reference.
- Introduce `IColumnFactory.makeColumnRandomInsertions(IAggregation, int)` which do not rely on sorting, useful when iteration order is random.

### Changed
- Prevent `QueryStepRecursiveAction` from executing multiple times shared tasks in a DAG.
- `AbstractAdhocMap` does not extend `AbstractMap` to ensure all default methods have optimal implementation.
- `DagCompletableExecutor` enable task sharing when evaluating a DAG of tasks.
- `IAppendableColumn.freeze()` turned to `IAppendableColumn.freeze(IFreezingStrategy)`
- `MultitypeNavigableColumn` propagates the capacity to values.
- Pivotable spring profiles has been reworked. There is more a default `application.yml`, but one should import `pivotable-config.yml`.

### Removed
- `ISliceFactory.newMapBuilder()` is removed. You need to have a clear view of the keyset.
- `IMultitypeColumnFastGet.set(T)` is removed. You can rely on `.append(T)` which has a clearer semantic.

## [0.0.14]

### Added
- Spring Actuator Health is enriched through `AdhocSchemaHealthIndicator`
- FSST String compression is a `IFreezingWithContext`. Relates with https://github.com/cwida/fsst
- Introduces `IAdhocMap.retainAll` to make an immutable copy with a subset of columns.

### Changed
- `ISliceFactory` actually enable compression, based on dictionarisation, or `long[]` or `int[]`, or FSST for `String`.

### Fixed
- `CubeQuery.CubeQueryBuilder.andFilter(String, Object)` now relies on `ColumnFilter.matchLax(String, Object)`.

## [0.0.13]

### Added
- Experimental support of [MongoDB](https://www.mongodb.com/docs/languages/java/)
- Introduce multiple `.executeAsync(...)` methods, as alternative to usual `.execute(...)` returning a `ListenableFuture`.
- Introduce `QueryPod.addCancellationListener(Runnable)`, enabling registration and cancellation of inner parts when cancelling the outer query.
- Pivotable will automatically cancel currently running queries if another query is triggered.
- `IAdhocMap.retainAll(Set<String>)` enables faster creation of `IAdhocMap` over a subset of columns (e.g. inducing).

### Changed
- Refactored `ISliceFactory`, preparing the ability to rely on dictionarized and column-oriented storage.

### Removed

- `DSLSupplier.fromConnection(Supplier<Connection>)` has been removed.

### Fixed
- `FilterOptimizer` may skip `or vs and` simplification (e.g. `FilterBuilder.or("a==a1&(b==b1|c==c1)").optimize()`).
- `FilterOptimizer` may skip `or vs and` simplification (e.g. `FilterBuilder.or("a=in=(a1,a2)&b==b1|a=in=(a1,a2)&!(b!=b2&!(c==c1&d==d1&b==b3))").optimize()`).
- `FilterStripper` was not properly stripping `OR` in case one stripped operand was `matchAll`.
- VueJS router is properly waited for (with `.isReady()`) to load the queryModel from `#hash` on relogging.

### Improved
- `TableQueryOptimizerSinglePerAggregator` now build a tree of intermediate CubeQuerySteps, to enable computing each filters a single time.
- `SimpleFilterEditor.shift` has been optimized.

## [0.0.12]

### Added
- Introduced `InternalQueryOptions.ONE_TABLE_QUERY_PER_AGGREGATOR` to force `TableQueryOptimizerSinglePerAggregator` from the options.
- Enable custom `ISliceFilter` in `FILTER` in `TableQueryV2`
- `AndFilter.copyOf` and `OrFilter.copyOf` enables very fast creation of `AndFilter` and `OrFilter` is all optimizations has to be skipped.
- Add `TableQueryEngine` information in the `EXPLAIN`. In particular, describe the `inducerToInduced` DAG similarly to `DagExplainer`.

### Changed
- Renamed `ITabularGroupByRecord.groupByKeySet()` into `ITabularGroupByRecord.columnsKeySet()`.

### Removed
- Removed `IAdhocSlice.getRawSliced(String)`. Prefer `IAdhocSlice.getGroupBy(String)`.
- Removed `Combinator implements IMayHaveColumnGenerator` as only `IDecomposition` seems a reasonable option. 
- Removed `GeneratedColumnsPreparator` as `IMayHaveColumnGenerator` are automatically added by `StandardQueryPreparator`

### Fixed
- Fix bug in SQL condition with NOT given NULL. (Typically `NOT(c = 'v')` is turned into `NOT(c IS NOT NULL AND c = 'v')`).
- Actuator configuration in Pivotable has been reviewed. `/actuator/info` and `/actuator/health` are public.
- Memory-leak in `CachingOperatorsFactory` due to `DispatchorQueryStep` adding values as option, for `CumulatingDecomposition`.
- Improve compaction in `IMultitypeMergeableGrid.closeColumn`

### Improved
- `FilterHelpers.getFilteredColumns` is faster by relying on `Stream.mapMulti`
- `FilterOptimizer.optimizeAndOfOr` skips operands which are alone on given column, even if they hold an `OR`.
- `TableQueryOptimizer.splitInducedAsDag` may register an additional CubeQueryStep in order to apply the `FILTER` clause only once for a step of induced steps.

## [0.0.11]

### Added
- Introduced `ISliceReader` to help reading coordinate given a `ISliceWithStep`. Usage is `slice.sliceReader().getValueMatcher(c)` instead of `FilterHelpers.getValueMatcher(slice.asFilter(), c)`.
- Introduced `FilterBuilder` to help building `ISliceFilter` with an explicit way to skip or not optimizations. It replaces `AndFilter.and` and `OrFilter.or` .
- Introduced `AdhocUnsafe.sliceFilterOptimizer` and `IFilterOptimizerHelpers` to enable customization of `ISliceFilter` optimizer.
- Enabled `ISliceFilter` over an `ICalculatedColumn`. It works by post-filtering after adding in groupBy underlying columns.
- Introduced `FilterEquivalencyHelpers` to help evaluating equivalent `ISliceFilter`. It is useful to workaround issues related to `ISliceFilter` optimizations, which leads to equivalent boolean expression having different representations.
- Introduced `FilterOptimizerHelpersWithCache.java`, to enable caching of intermediate `ISliceFilter`.
- Introduced `JooqTableWrapper.fieldsCache` to prevent fetching all columns on each query (especially on `CompositeCube` scenario).
- Introduced `IFilterStripper` to enable customization and cache-friendly behavior around `stripWhereFromFilter` and `isStricterThan` methods.

### Changed
- `ITableQueryEngine` is now a field of `CubeQueryEngine` (instead of `.makeTableQueryEngine` factory method).
- `OrFilter.or` now relies on `AndFilter.and` to apply the same set of optimizations.
- `CompositeCubesTableWrapper` now rejects queries referring to unknown columns (in `groupBy` or `filter`).
- `CompositeCubesTableWrapper` now apply filters on unknown column in subCube as if the whole subCube has the `onMissingColumn` value.
- `ITableTranscoder` is renamed into `ITableAliaser`. Relates classes are also renamed (`MapTableTranscoder -> MapTableAliaser`, etc).
- `MapTableAliaser.queriedToUnderlying(...)` is renamed `MapTableAliaser.aliasToOriginal(...)`.
- Update `DuckDB` to `1.4.0.0` (from `1.3.2.0`).
- `AndFilterBuilder.filter` is renamed `AndFilterBuilder.and`.
- `OrFilterBuilder.filter` is renamed `OrFilterBuilder.and`.
- `TableQueryOptimizerSinglePerAggregator` now rely on `TableQueryOptimizer` to evaluate rootInducers.
- `ColumnFilter.isEqualTo` is renamed `ColumnFilter.matchEq`.

### Fixed
- Prevent `AdhocFactories` using an invalid `ICoordinateNormalizer` by default, using `StandardSliceFactory` own default.
- Prefer `array.concat(otherArray)` over `arrat.push(...otherArray)` to improve performance and prevent `max call stack size` (https://github.com/vuejs/core/issues/13826).
- `TableQueryEngineBootstrapped` will properly skip steps available from `IQueryStepCache`.
- Alternative `ITableQueryOptimizer` like `TableQueryOptimizerSinglePerAggregator` were broken due to lack of flexibility in sanity checks.

## [0.0.10]

### Added
- Introduced `ISliceFactory` to enable custom way of creating `Map` and `IAdhocSlice`.
- Introduced `ICoordinateNormalizer` to centralize how coordinates should be normalized (e.g. `int` into `long`).
- Introduced `NullMatcher.NULL_HOLDER` to represent `null` where a null-reference is forbidden.
- Introduced `ICalculatedCoordinate` to enable adding coordinate into columns. Typical usecase is having an `*` coordinate aggregating all other coordinates, as a grandTotal of current column.
- Playwright is activated to integration-tests Pivotable.

### Changed
- Update `DuckDB` to `1.3.2.0` (from `1.3.1.0`)
- `ITabularRecord.getGroupBys()` now returns a `IAdhocMap`
- Most `SliceAsMap` usages are replaced by `IAdhocSlice`, and `IAdhocSliceWithStep` does not extends `IAdhocSlice` anymore.
- `IAdhocFilter.MATCH_ALL` is now serialized as `"matchAll"`
- `IAdhocFilter.matchAll` is now serialized as `"matchNone"`
- `IAdhocFilter` is renamed `ISliceFilter`

### Fixed
- Various mis-behaviours related to `null` as `GROUP BY` coordinate.

## [0.0.9]

### Added
- Columns can be tagged through `AdhocSchema`.
- In Pivotable, tag filtering is now cross-columns, in addition to cross-measures.
- Additional `WorldCup` example cube, with more complex measures.
- In Pivotable, clicking a column header will sort rows based on given column.
- Introduces `ICompactable`. It is called on each queryStep of the DAG.
- Introduces `ShiftedValueMatcher`, enabling `SimplerFilterEditor.shiftIfPresent` to shift dynamically existing values.
- Introduces `CumulatingDecomposition`, which is another case of `Dispatchor`. Can be used for cumulative `SUM` through time.
- Introduces `JooqTableQueryFactory.canFilterAggregates`, to generate `SUM(CASE y THEN x)` instead of `SUM(x) FILTER (y)`, as many Databases (like `RedShift` does not support `FILTER`).
- Introduced `JooqTableCapabilities` to configure the SQL capabilities on the underlying table.

### Changed
- `IColumnMatcher.toString()` has specialized implementations, depending on the matcher type (e.g. `c==v` for `EqualsMatcher`).
- `Bucketor` is renamed `Partitionor`.
- `StandardQueryOptions.CONCURRENT` is revamped for `CubeQuerySteps` by relying on a `RecursiveAction`.
- `DefaultCustomTypeManager` is renamed `StandardCustomTypeManager`.
- `DefaultMissingColumnManager` is renamed `StandardMissingColumnManager`.
- `DefaultQueryPreparator` is renamed `StandardQueryPreparator`.
- `DuplicatingDecomposition` is revamped regarding management of filtered-but-not-grouped-by columns.
- Update `DuckDB` to `1.3.1.0` (from `1.3.0.0`)
- `AdhocUnsafe.defaultCapacity()` is renamed `AdhocUnsage.defaultColumnCapacity()`
- `AtotiMeasureToAdhoc` now supports non-`String` properties.
- `MultitypeHashColumn.purgeAggregationCarriers()` now returns a purged copy, instead of mutating current instance.
- `InMatcher` and `ComapringMatcher` now normalize `long-like` into `long` and `double-like` into `double`.
- `AdhocPrimitiveHelpers.isLongLike` over `BigDecimal` should never returns true. `BigInteger` and `BigDecimal` are manage separately.

### Fixed
- `JooqTableQueryFactory` now supports `OrMatcher`, `AndMatcher` and `NotMatcher`.
- `ReversePolishCombination` now supports complex objects are subFormula results. More generally, subFormula results are are `.toString()` anymore.
- `StandardOperatorFactory.enrichOptions` now relies on some root `IOperatorsFactory`, especially relevant when using a `CompositeOperatorFactory`.

## [0.0.8]
### Added
- `CompositeTableTranscoder` is introduced.
- `IDecompositionEntry` is introduced.
- `IDagBottomUpStrategy` is introduced.
- `IDagBottomUpStrategy` is introduced.
- `AdhocFactories` is introduced.

### Removed
- `AtotiTableTranscoder.priorityTranscoder` is removed. Use `CompositeTableTranscoder` instead.
- `InMemoryTable_Recording` is removed. Use `InMemoryTable.throwOnUnknownColumn` instead.

### Changed
- `IAdhocColumn` now relies on `JsonTypeInfo.Id.MINIMAL_CLASS`
- Upgrade `DuckDB` to `1.3.0.0` (from `1.2.2.0`)
- Upgrade `SpringBoot` to `3.5.0` (from `3.4.6`)
- A not-Aggregator measure without underlying is automatically relying on the `empty` measure
- `FirstNotNullAggregation` renamed into `CoalesceAggregation`. Its key changed from `FIRST` to `COALESCE`
- `FirstNotNullCombination` renamed into `CoalesceCombination`. Its key changed from `FIND_FIRST` to `COALESCE`
- `LikeMatcher.like` renamed into `LikeMatcher.pattern`
- `AdhocUnsafe.failFast` renamed into `AdhocUnsafe.isFailFast()`
- `AtotiMeasureToAdhoc` should be extended with `@SuperBuilder` and a custom SuperBuilder.

## [0.0.7]
### Added
- `CachingTableWrapper` can be used to add a caching layer on top of a `ITableWrapper`. It can be skipped with `StandardQueryOptions.NO_CACHE`.
- `CompositeCubesTableWrapper` now supports `FILTER` statements, which are handled with a transient `Filtrator` measure.

### Changed
- `AdhocQuery` is renamed `CubeQuery`
- `AdhocQueryEngine` is renamed `CubeQueryEngine`
- `dag` sub-package is renamed `engine`
- `ExecutingQueryContext ` is renamed `QueryPod`
- Improved `JooqSnowflakeSchemaBuilder` to better handle edge cases like fields with a `.` in their name.
- `QueryPod` now relies on `MoreExecutors.newDirectExecutorService()` by default, which is used by not `StandardQueryOptions.CONCURRENT` queries.
- Rename `-Dadhoc.limitColumnLength=1024` into `-Dadhoc.limitColumnSize=1024`

### Fixed
- `TopologicalOrderSpliterator` is not `parallel` by default, as it should be triggered by `StandardQueryOptions.CONCURRENT`.

## [0.0.6]
### Fixed
- `SliceAsMap` did not handle a column with different `Comparable`, typically happening on `LocalDate` column with a `NULL` String on failed JOIN.
- `not` now works with `FilterHelpers.getFilteredColumns(IAdhocFilter)` and `JooqTableQueryFactory.toCondition(IAdhocFilter)`

### Changed
- `DuckDB` is upgraded to 1.2.2.0
- In `Pivotbale`, `customMarkers` are written on a per-value basis, with the `path` as key.
- `QueryTableV2` introduced querySharing for `TableQuery` with same groupBy, by relying on SQL `FILTER` keyword.
- Drops `CharSequence` as a privileged type (e.g. in aggregations, in `IMultiTypeXXX`)

### Added
- Enable `cubeSlicer` in `CompositeCubesTableWrapper`: it enables filtering by underlying cube
- Enable Export to CSV in `Pivotable`
- Enable Export to Clipboard in `Pivotable`

## [0.0.5]
### Changed
- `Pivotable` opens a filter-helper when double-cliking a cell of the grid.
- `Pivotable` search now highlights/marks matching elements.
- `StandardQueryOptions.CONCURRENT` has now much better behavior, especially around composite cubes, multiple tableQueries, and DAG with independent steps.
- Prepare querySharing for aggregators with same groupBy but different filters, with `FILTER` sql keyword. See `QueryTableV2`
- `ExpressionCombination` can refer to underlyings by their index with the syntax `underlyings[index]`

### Added
- Enable `customMarkerMetadata` in `AdhocSchema`
- Enable `tags` measure filtering in `Pivotable`
- Introduces `AdhocAutoConfiguration` for easy Spring auto-configuration.
- `AdhocQuery`s executed through `AdhocSchema` now see their `AdhocFilter` being transcoded (e.g. to manage a filter over a `date`, expressed as a JSON String).

### Removed
- Remove `StandardQueryOptions.RETURN_UNDERLYING_MEASURES` as it is meaningful only for aggregators and combinators

## [0.0.4]
### Changed
- `IQueryOption` is now part of `IAdhocQuery`
- `debug` and `explain` are now only togglable as `IQueryOption`
- Normalize the way columns are parsed into SQL names (by relying on JooQ `Parser.parseName`).
- Improve `.getColumns` relatively to various column customizations (e.g. customType, calculatedColumn, etc)
- Improve `EXPLAIN` by reporting the queried cube, the queryId and its parentQueryId.

### Added
- Enable complex filtering, by additional groupBy from table and manual post-filtering (e.g. `JooqTableQueryFactory.ConditionWithFilter`)
- Enable `CalculatedColumn` which can evaluate additional columns based on table columns.
- Introduce `IImplicitOptions` in `DefaultQueryPreparator`
- Initial work around `CONCURRENT` option.

## [0.0.3]
### Added
- `ForestAsGraphvizDag`: can turn an `IMeasureForest` into a Graphviz DOT file.
- `ICubeWrapper.getCoordinates` can query multiple columns in one call, which is leveraged by `DuckDb` tables.
- `Pivotable` can now drop column/measure from grid headers.
- Fix serialization of `IQueryOptions`, and update `Pivotable` for proper options.

## [0.0.2]
### Added
- Enables m2central deployments: https://central.sonatype.com/artifact/eu.solven.adhoc/adhoc

## [0.0.1]
### Added
- Initial version
