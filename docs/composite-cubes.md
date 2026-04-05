# Composite Cubes

`CompositeCubesTableWrapper` unifies several `ICubeWrapper` instances into a single virtual table.
Each sub-cube is queried independently; their results are concatenated and re-aggregated by the
composite layer. The caller sees one coherent data source.

This is the preferred approach when you have multiple simpler cubes covering different data sets or
different schemas, and you want to query across all of them without merging them into a single,
more complex cube.

---

## Motivation

Consider two cubes fed by different data sources:

|   Cube   |           Columns           |    Measures    |
|----------|-----------------------------|----------------|
| `equity` | `desk`, `ccy`, `instrument` | `pnl`, `delta` |
| `rates`  | `desk`, `ccy`, `tenor`      | `pnl`, `dv01`  |

A user query `SUM(pnl) GROUP BY desk` should aggregate both. With a composite wrapper, each cube
contributes its own `pnl` slice; the composite layer sums them together. Neither cube needs to know
about the other's schema columns (`instrument`, `tenor`).

---

## Building a composite

```java
CompositeCubesTableWrapper composite = CompositeCubesTableWrapper.builder()
		.name("all-books")
		.cube(equityCube)
		.cube(ratesCube)
		.build();
```

The composite wrapper itself is used as the `ITableWrapper` of an outer `CubeWrapper`:

```java
CubeWrapper outerCube = CubeWrapper.builder()
		.name("composite")
		.table(composite)
		.forest(compositeForest)
		.build();
```

To automatically register all measures from sub-cubes into the outer forest, use
`injectUnderlyingMeasures()`:

```java
IMeasureForest compositeForest = composite.injectUnderlyingMeasures(MeasureForest.builder().build());
```

This walks every sub-cube's measure forest and registers each measure in the composite. If two
sub-cubes declare a measure with the same name, both are retained as `measure.cubeName` variants to
avoid collisions.

---

## How queries are dispatched

For each incoming `TableQuery`, the composite wrapper:

1. **Determines eligibility** — a sub-cube is included only if it can contribute at least one
   requested measure or column. Cubes that have no overlap are skipped entirely.
2. **Translates the query per cube** — the groupBy and filter are projected to the columns that
   the sub-cube actually knows about. Unknown columns are dropped from the sub-query.
3. **Executes sub-queries** — concurrently by default (virtual threads via the query's
   `ExecutorService`).
4. **Fills missing columns** — coordinates for columns unknown to a given cube are injected as
   mask values (managed by `IColumnsManager`), so all result rows share a uniform schema.
5. **Concatenates results** — slices from all eligible cubes are streamed together and
   re-aggregated by the outer cube's measure forest.

---

## Schema differences between sub-cubes

Sub-cubes are allowed to have different columns. The composite layer tags each column with its
coverage:

|             Tag              |              Meaning              |
|------------------------------|-----------------------------------|
| `composite-full`             | Column present in every sub-cube  |
| `composite-partial`          | Column present in some sub-cubes  |
| `composite-known:cubeName`   | Column present in the named cube  |
| `composite-unknown:cubeName` | Column absent from the named cube |

When a sub-cube does not carry a column that appears in the composite groupBy, the missing value is
filled by `IColumnsManager.onMissingColumn()`. The default behaviour returns `null`; you can
override it per column to return a sentinel value (e.g. `"N/A"`, `0`) that keeps the aggregation
meaningful.

---

## Identifying which cube contributed a row

The virtual column `~CompositeSlicer` (configurable via `optCubeSlicer`) holds the name of the
sub-cube that produced each slice. You can group or filter by it:

```java
CubeQuery.builder()
		.groupBy(GroupByColumns.named("desk", "~CompositeSlicer"))
		.measure("pnl")
		.build()
```

This lets you compare contributions side-by-side without changing the sub-cube definitions.

---

## Typical use cases

- **Different asset classes** sharing a few common dimensions (`desk`, `ccy`) but with
  asset-class-specific columns (`tenor`, `instrument`, `strike`).
- **Different time horizons or scenarios** maintained as separate cubes for isolation, queried
  together for reporting.
- **Incremental data sources** — a historical cube and a real-time cube unified behind a single
  query endpoint.

In all these cases the composite wrapper avoids the need to design a unified schema upfront: each
cube remains simple and focused, and the composite layer handles the fan-out and re-aggregation.

---

## Further reading

- `HelloCompositeCubes` — minimal self-contained example with two in-memory cubes of different schemas
- `TestCompositeCubesTableWrapper` — comprehensive test suite covering filtering, grouping, and schema differences
- `TestCompositeCubesTableWrapper_Concurrent` — verifies that sub-queries execute in parallel
- `TestTableQuery_DuckDb_CompositeCube` — integration test with DuckDB-backed sub-cubes
- [Concepts → General Architecture](concepts.md) — how `ICubeWrapper`, `ITableWrapper`, and `IMeasureForest` relate

