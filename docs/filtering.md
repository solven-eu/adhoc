# Filtering

A `ISliceFilter` is a way to restrict the data to be considered on a per-column basis. The set of filters is quite small:

- `AndFilter`: an `AND` boolean operation over underlyings `ISliceFilter`. If there is no underlying, this is a `.matchAll`.
- `OrFilter`: an `OR` boolean operation over underlyings `ISliceFilter`. If there is no underlying, this is a `.matchNone`.
- `NotFilter`: an `!` or `NOT` boolean operation over underlying `ISliceFilter`.
- `IColumnFilter`: an operator over a specific column with given `IValueMatcher`.

A `IValueMatcher` applies to any `Object`. The variety of `IValueMatcher` is quite large, and easily extendible:

- `EqualsMatcher`: true if the input is equal to some pre-defined `Object`.
- `NullMatcher`: true if the input is null.
- `LikeMatcher`: true if the input `.toString` representation matching the registered `LIKE` expression. [www.w3schools.com](https://www.w3schools.com/sql/sql_like.asp)
- `RegexMatcher`: true if the input `.toString` representation matching the registered `regex` expression.
- [etc](https://github.com/search?q=repo%3Asolven-eu%2Fadhoc+%22implements+IValueMatcher%22&type=code)

## Implementing custom operators

Adhoc provides a `StandardOperatorFactory` including generic operators (e.g. `SUM`).

- It can refer to custom operators by referring them by their `Class.getName()` as key.
- Your custom `IAggregation`/`ICombination`/`IDecomposition`/`ISliceFilterEditor` should then have:
  - Either an empty constructor
  - Or a `Map<String, ?>` single-parameter constructor.

One may also define a custom `IOperatorsFactory`:

- by extending it
- by creating your own `IOperatorsFactory` and combining with `CompositeOperatorFactory`
- by adding a fallback strategy with `DummyOperatorFactory`

## About performance

Humans are generally happier when things goes faster. `Adhoc` enables split-second queries over the underlying table. Very large queries can be performed with limited resources (e.g. a JVM with a few GB of RAM) and may take seconds/minutes.

The limiting factor in term of performance is generally the under table, which executes aggregations at the granularity requested by Adhoc, induced by the User `GROUP BY`, and those implied by some formulas (e.g. a `Partitionor` by Currency). 

Hence, we do not target absolute performance in `Adhoc`. In other words, we prefer things to remains slightly slower, as long as it enables this project to remains simpler, given a query is generally slow due to the underlying `ITableWrapper`.

Adhoc performances can be improved by:
- Scale horizontally: each Adhoc instance is stateless, and can operate a User-query independently of other shards. There is no plan to enable a single Adhoc query to be dispatched through a cluster on Adhoc instance, but it may be considered if some project would benefit from such a feature.
- Enable caching (e.g. `CubeQueryStep` caching).

### Concurrency

Concurrency is enabled by default (`StandardQueryOptions.CONCURRENT`), but it can be disabled through `StandardQueryOptions.SEQUENTIAL`.

Non-concurrent queries are executed in the calling-thread (e.g. `MoreExecutors.newDirectExecutorService()`).

Concurrent queries are executed in Adhoc own `Executors.newWorkStealingPool`. It can be customized through `AdhocUnsafe.adhocCommonPool`.

Concurrent sections are:

- subQueries in a `CompositeCubesTableWrapper`: each subCube may be queried concurrently.
- `CubeQuerySteps` in a DAG: independent tasks may be executed concurrently.
- tableQueries induced by leaves `CubeQUerySteps`: independent tableQueries may be executed concurrently.

If you encounter a case which performance would be much improved by multi-threading, please report its specificities through a new issue. A benchmark / unitTest demonstrating the case would be very helpful.

`parallelism` can be configured in `AdhocUnsafe.parallelism` or through `-Dadhoc.parallelism=16`.
