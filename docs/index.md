# Welcome to Adhoc

This project develops an in-memory transformation engine to enable computations of complex KPIs on top of any aggregation engine.

## Pivotable

Pivotable is a referential implementation of a web application wrapping Adhoc capabilities.

## Documentation

### Core concepts

- [Lexicon](lexicon.md) — key terms and concepts used throughout the project
- [Concepts](concepts.md) — general architecture, query flow, measures, columns, and transcoders
- [Architecture](architecture.md) — class hierarchy diagram generated from source
- [Slice and IAdhocMap](slice.md) — what a slice is, how `IAdhocMap` backs it, perfect-hash key lookup and cached projections

### Working with data

- [Tables](tables.md) — table query DAG, SQL/JooQ integration, null handling
- [Filtering](filtering.md) — `ISliceFilter`, `IValueMatcher`, custom operators, and performance notes
- [Type Inference](type-inference.md) — int→long and float→double promotion rules

### Advanced topics

- [CubeQueryEngine](cube-query-engine.md) — two-DAG workflow: Cube DAG (measure logic) and Table DAG (DB query optimisation)
- [Immutability](immutability.md) — why immutable data structures improve correctness, caching, and thread safety
- [Concurrency](concurrency.md) — thread-pool topology, pool selection rules, and `IAdhocStream` model
- [Optimisations](optimisations.md) — data-structure and query optimisations (slices, perfect hashing, encodings, filter arithmetic)
- [Data Transfer](data-transfer.md) — primitive type management (`IValueReceiver`, `IValueProvider`)
- [Unsafe](unsafe.md) — advanced tweaks and low-level configuration via `AdhocUnsafe`

### Customisation

- [Operators Factory](operators-factory.md) — `IOperatorsFactory` and injecting custom aggregations/combinations
- [Custom Aggregations](aggregation.md) — implementing `IAggregation` for domain objects; `MarketRiskSensitivity` example
- [ICombination](combination.md) — the simplest business-logic extension point; `ISliceWithStep.sliceReader()` and the GROUP BY coordinate guarantee
- [Partitionor](partitionor.md) — GROUP BY widening, per-partition combination, and the ForeignExchange use case
- [Filtrator](filtrator.md) — enforcing a constant hardcoded filter on top of the query filter
- [Unfiltrator](unfiltrator.md) — widening a filter to a coarser granularity; share-of-total and hierarchical totals
- [Shiftor](shiftor.md) — fetching data from a different slice via `IFilterEditor`; previous-day and business-day patterns
- [Custom Markers](custom-marker.md) — per-query user context propagated to every `CubeQueryStep`; raw-map → typed-POJO transcoding via `ICustomMarkerTranscoder`; Spring-side wiring through `IAdhocSchemaCustomizer`
- [Calculated Columns](calculated-columns.md) — `IColumnGenerator` / `IDecomposition` (EXPLODE), `FunctionCalculatedColumn` (per-record function), `EvaluatedExpressionColumn` (user-authored expression), and `ColumnsManager` auto-propagation
- [Hierarchies](hierarchies.md) — modelling multi-level dimensions today and the roadmap for native hierarchy support
- [Many-to-many](many-to-many.md) — how a single fact can contribute to multiple coordinates via `Dispatchor`
- [Custom Measures](custom-measure.md) — implementing `IHasUnderlyingMeasures` + `AMeasureQueryStep` for arbitrary evaluation logic; `RouterMeasure` example
- [Composite Cubes](composite-cubes.md) — unifying multiple `ICubeWrapper` instances via `CompositeCubesTableWrapper`
- [Authorizations](authorizations.md) — rights management via `IImplicitFilter`

### Project reference

- [Module Dependencies](dependencies.md) — inter-module Maven dependency graph
- [Debug / Investigations](debug.md) — debug/explain query options and automated documentation tools
- [FAQ](faq.md) — recurrent cases with one or more solutions
- [Research](research.md) — background reading and algorithmic references

