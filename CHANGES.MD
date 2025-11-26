This document is intended for Adhoc developers and users.

We adhere to the [keepachangelog](https://keepachangelog.com/en/1.0.0/) format

## [0.0.13]

### Added

### Changed

### Removed

### Fixed
- `FilterOptimizer` may skip `or vs and` simplification (e.g. `FilterBuilder.or("a==a1&(b==b1|c==c1)").optimize()`)

### Improved
- `TableQueryOptimizerSinglePerAggregator` now build a tree of intermediate CubeQuerySteps, to enable computing each filters a single time.

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
