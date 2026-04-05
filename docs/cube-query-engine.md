# CubeQueryEngine

`CubeQueryEngine` is the orchestrator that turns a `CubeQuery` into a `ITabularView`. It does
this in two distinct phases, each built around its own directed acyclic graph (DAG):

1. **Cube DAG** — a DAG of `CubeQueryStep` objects, one per distinct `(measure, groupBy, filter)`
   triple, built top-down from the requested measures.
2. **Table DAG** — a DAG of `TableQueryStep` objects built from the leaf aggregators of the Cube
   DAG, optimised to minimise round-trips to the underlying `ITableWrapper`.

The two DAGs are executed sequentially: the Table DAG runs first and fills in the aggregator
results; the Cube DAG then evaluates measure logic bottom-up.

---

## Phase 1 — Building the Cube DAG

The entry point is `CubeQueryEngine.makeQueryStepsDag(QueryPod)`, which delegates the recursive
expansion to `QueryStepsDagBuilder`.

### Root steps

`getRootMeasures(QueryPod)` translates the user's measure list into a set of root `CubeQueryStep`
objects — one per requested measure, all sharing the query's top-level `groupBy` and `filter`.

### Recursive expansion via getUnderlyingSteps()

For each pending step the builder asks the measure what it depends on:

```
if measure is Aggregator    → leaf node; no further expansion
if measure is IHasUnderlyingMeasures →
    factory.makeQueryStep(step, measure)   // instantiates the IMeasureQueryStep
    queryStep.getUnderlyingSteps()         // returns child CubeQueryStep list
    register each child and recurse
```

This is the seam where custom measures plug in — `getUnderlyingSteps()` declares the children
(see [Custom Measures](custom-measure.md)).

The DAG is a proper DAG, not a tree: two measures that depend on the same underlying with the
same groupBy/filter share a single `CubeQueryStep` node. Deduplication happens during
`registerDescendants()`.

### What a CubeQueryStep carries

```java
CubeQueryStep {
    IMeasure  measure;      // which measure to evaluate at this node
    IGroupBy  groupBy;      // the columns defining the result granularity
    ISliceFilter filter;    // the filter in scope at this node
    Object    customMarker; // optional context (e.g. target currency for a Shiftor)
}
```

Partitionors widen the `groupBy` when creating child steps; Filtrators and Shiftors modify the
`filter`; Combinators pass both through unchanged.

---

## Phase 2 — Executing the Table DAG

Once the Cube DAG is fully expanded, all leaf nodes whose `IMeasure` is an `Aggregator` are
collected and handed to `TableQueryEngine.executeTableQueries()`.

### The Table DAG: inducers and induced

The `TableQueryEngine` does not send one query per leaf `CubeQueryStep`. Instead,
`ITableQueryFactory.splitInduced()` builds a second DAG:

```
Set<TableQueryStep>              (one per leaf aggregator)
        ↓  splitInducedAsDag()
IAdhocDag<TableQueryStep>        (inducer → induced edges)
        ↓  IMergeInducers
Set<TableQueryV4>                (the actual DB queries)
```

**Inducer vs induced.** A `TableQueryStep` A *induces* B when A's result is a strict superset of
B's — for example, `SUM(pnl) GROUP BY desk, ccy` induces `SUM(pnl) GROUP BY desk` because the
second can be derived by re-aggregating the first. When this is the case, B is never sent to the
database; Adhoc computes it directly from A's result. This is often faster than a DB round-trip
and produces a simpler query plan.

**Merging inducers.** `IMergeInducers` combines multiple `TableQueryStep` objects into a single
inducer where possible, by:

- Taking the union of all `groupBy` column sets.
- Taking the OR of all `filter` clauses.

The merged inducer is sent to the table as one query; each original step is then derived from its
result by filtering and re-aggregating. The goal is fewer, wider queries rather than many narrow
ones.

### Execution

The inducers (real DB queries) are executed concurrently via the query's `ExecutorService`. Once
an inducer completes, `ITableQueryInducer.evaluateInduced()` computes the induced steps from its
result. This produces a `Map<TableQueryStep, ICuboid>` that fills in all the aggregator leaves of
the Cube DAG.

---

## Phase 3 — Walking up the Cube DAG

With aggregator results in hand, `walkUpDag()` traverses the Cube DAG bottom-up. For each
non-aggregator step, `IMeasureQueryStep.produceOutputColumn(underlyings)` is called with the
already-computed cuboids of its children, producing a new cuboid for that step.

Independent steps are executed concurrently via `DagCompletableExecutor`, which uses
`CompletableFuture.allOf()` to wait for a step's dependencies before scheduling it:

```
for each step (topological order, concurrent where independent):
    wait for all child cuboids
    call queryStep.produceOutputColumn(childCuboids)
    publish result cuboid for parent steps to consume
```

The root step's cuboid is finally converted into an `ITabularView` and returned to the caller.

---

## Full flow diagram

```
CubeQuery
    │
    ▼
getRootMeasures()
    │
    ▼
QueryStepsDagBuilder.registerRootWithDescendants()
    │   ↳ for each IHasUnderlyingMeasures: getUnderlyingSteps() → recurse
    │   ↳ for each Aggregator: leaf node
    │
    ▼
Cube DAG  (CubeQueryStep nodes)
    │
    ├─► executeTableQueries()
    │       │
    │       ▼
    │   splitInduced() + mergeInducers()
    │       │
    │       ▼
    │   TableQueryV4 → ITableWrapper (DB)
    │       │
    │       ▼
    │   evaluateInduced() → Map<TableQueryStep, ICuboid>
    │
    ▼
walkUpDag()  (concurrent, bottom-up)
    │   ↳ produceOutputColumn(childCuboids) per non-aggregator step
    │
    ▼
ITabularView
```

---

## Roadmap: unified DAG

Today the Cube DAG and the Table DAG are built and optimised independently. A known limitation is
that optimisations which span both DAGs are not possible — for example, a Partitionor that widens
the groupBy might create a `TableQueryStep` that could serve another measure's aggregator, but the
two optimisers never see each other's context.

A roadmap item tracks this:

> **[Performance] Merge the CubeQueryStep DAG and the TableQueryStep DAG into a single unified
> execution graph.** This would allow the combined optimiser to discover shared sub-computations
> across the two layers, enabling further reduction of DB round-trips and elimination of redundant
> Adhoc-side re-aggregations.

---

## Further reading

- [Concepts → General Query Flow](concepts.md) — higher-level overview with Mermaid diagram
- [Concurrency](concurrency.md) — thread-pool topology and `DagCompletableExecutor`
- [Custom Measures](custom-measure.md) — implementing `getUnderlyingSteps()` to define Cube DAG edges
- [Optimisations](optimisations.md) — details on table-query merging and inducer logic
