# Lexicon

Definitions of terms used throughout Adhoc, with cross-references to standard OLAP vocabulary and
to ActivePivot/Atoti where the concept maps (or deliberately diverges).

---

## OLAP

**Online Analytical Processing.** A class of read-only queries that synthesise large volumes of raw
data into a smaller, aggregated result set — typically via `GROUP BY`, filters, and computed
indicators. Contrasts with OLTP (transactional writes).

---

## Cube

A multidimensional data space where every point (a *cell*) is identified by a coordinate along each
*column* and holds one or more *measure* values.

In Adhoc, `ICubeWrapper` pairs a `table` (data source) with a `forest` (measure tree). Queries are
issued against the cube; the cube decomposes them into `TableQuery` objects that are sent to the
table, then assembles the results through the measure tree.

> **ActivePivot equivalent:** `IActivePivot` / `IMultiVersionActivePivot`.

---

## Table

The external data source backing a cube. Adhoc treats tables as black boxes that can answer
aggregation queries (`TableQuery`): `GROUP BY` a set of columns, `WHERE` a filter, compute a set of
aggregations.

`ITableWrapper` is the abstraction. Implementations include:

| Implementation | Backend |
|---|---|
| `InMemoryTable` | `List<Map>` in JVM heap |
| `JooqTableWrapper` | Any JDBC-compatible engine (DuckDB, PostgreSQL, Redshift, …) |
| `MongoTableWrapper` | MongoDB |
| `ActivePivotTableWrapper` | Atoti/ActivePivot cube queried as a table |

> **ActivePivot equivalent:** a *store* is the closest analogue, though ActivePivot stores are
> strongly-typed and schema-bound, while Adhoc tables are schema-on-read.

---

## Column

The unit of grouping and filtering. Roughly equivalent to a *dimension* or *hierarchy level* in
traditional OLAP, but Adhoc deliberately flattens the hierarchy model: every column is a single
level — there are no multi-level hierarchies.

A column can be:
- **Physical** — directly backed by a table column.
- **Calculated** (`ICalculatedColumn`) — derived at query time from other columns (e.g. a date
  truncation, a bucket expression).

> **Traditional OLAP vocabulary:** dimension → hierarchy → level. Adhoc collapses these three into
> one concept.  
> **ActivePivot equivalent:** a *level* within a hierarchy.

---

## Slice

A specific coordinate in the cube — a `Map<column, value>` that identifies a single cell or a set
of cells. In code, `ISlice` / `ISliceWithStep` carry these coordinates through the measure
evaluation pipeline.

---

## GroupBy

The set of columns along which a query is broken down. Equivalent to the `GROUP BY` clause in SQL
or the *axes* / *location wildcards* in MDX.

> **ActivePivot equivalent:** the wildcard dimensions of an `ILocation`.

---

## Filter

A boolean predicate over column values, restricting which rows of the table contribute to the
result. `ISliceFilter` implements full boolean arithmetic (AND, OR, NOT, column predicates).

Filters compose: a `Shiftor` or other transformator may narrow or widen the filter as it propagates
down the measure tree.

> **ActivePivot equivalent:** `ISubCubeProperties` / `ICubeFilter`.

---

## Cell

The intersection of a specific *slice* (set of column values) and a *measure* in the result of a
cube query. A cell holds the aggregated value for that combination.

---

## Measure

An indicator computed by the cube. The term covers three related concepts:

| Concept | Description | Example |
|---|---|---|
| **Archetype** | The type of formula, defined by its `QueryStep` class | `Combinator`, `Partitionor`, `Filtrator` |
| **Combination / implementation** | The concrete formula without its configuration | `SumCombination implements ICombination` |
| **Measure instance** | A named, configured instance ready to be queried | `Combinator.builder().name("revenue").combinationKey("SUM").underlyings(List.of("amount")).build()` |

> **ActivePivot equivalent:** a *measure* (native or post-processed).

### Aggregator

A leaf measure that queries the table directly and applies an aggregation function (SUM, MAX, COUNT,
…). `IAggregator` / `AggregatedMeasure`.

> **ActivePivot equivalent:** `AggregatedMeasure`.

### Combinator

A measure that combines the values of one or more underlying measures at the cell level — the most
common type of post-processing. Equivalent to `ABasicPostProcessor` in ActivePivot.

### Partitionor

A measure that re-aggregates underlying values across a sub-partition of the current groupby —
useful for running totals, share-of-total, etc. Equivalent to
`ADynamicAggregationPostProcessorV2`.

### Filtrator

A measure that applies an additional filter before evaluating its underlying — equivalent to
`AFilteringPostProcessorV2`.

### Dispatchor

A measure that dispatches the current slice into multiple derived slices before evaluating an
underlying. Equivalent to `AAdvancedPostProcessorV2`.

---

## Forest

A directed acyclic graph (in practice a tree) of measures. Each node is a measure; edges represent
*depends-on* relationships. The forest is the schema of all computable indicators for a given cube.

`IMeasureForest` / `IMeasureForestVisitor`.

---

## TableQuery

A query sent to an `ITableWrapper`. It specifies:
- a `GROUP BY` (set of columns)
- a `WHERE` filter
- a set of aggregations to compute

The query engine may merge or split `TableQuery` objects to minimise round-trips to the underlying
store.

---

## CubeQuery / CubeQueryStep

A `CubeQuery` is the user-facing request to a cube: which measures, which groupby, which filter.

Internally, the engine decomposes it into a DAG of `CubeQueryStep` objects — one per distinct
`(measure, groupby, filter)` triple. Steps share sub-computations where possible; the DAG is
executed concurrently by `DagCompletableExecutor`.

---

## MDX

**MultiDimensional eXpressions** — the query language standardised for OLAP cubes (used by
Microsoft Analysis Services, ActivePivot, and others). Adhoc does **not** implement MDX; queries
are expressed programmatically via the Java builder API or through the REST/GraphQL interface of
Pivotable.

---

## Pivotable

The reference Spring Boot web application wrapping Adhoc. It exposes cube queries over HTTP (REST +
optional GraphQL) and provides a Vue.js front-end. `Pivotable` is to `Adhoc` what a web server is
to a query engine.
