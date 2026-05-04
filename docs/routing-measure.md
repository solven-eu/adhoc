# RoutingMeasure — A Walkthrough

This page walks through the actual `RoutingMeasure` shipped in `engine/recipes`, focusing on the
practical tricks you hit when writing a custom measure: where the runtime wiring lives, what
Jackson can and cannot serialize, how `getUnderlyingSteps` shapes the DAG, what
`produceOutputColumn` is allowed to assume, and why the cross-step combine is *coalesce*, not SUM.

For the abstract pattern guide and the full contract surface, see
[Custom Measures](custom-measure.md). This page is the worked-example companion.

---

## What `RoutingMeasure` does

A `RoutingMeasure` decomposes each `CubeQueryStep` into one or more underlying steps via a
caller-supplied `IRoutingLogic`. The typical use case is a migration (e.g. Atoti → Adhoc) where
one logical measure must map to different physical measures or filters depending on the slice
— one underlying for data before a modelling cutoff, another for data after it.

```java
RoutingMeasure.builder()
    .name("dRouted")
    .underlying("d_legacy")            // covering set of underlying names...
    .underlying("d_modern")            //   ...the routingLogic may reference
    .routingLogic(step -> {
        // Decompose into legacy + modern, each with the appropriate date filter.
        ISliceFilter before = ColumnFilter.builder().column("date")
                .matching(ComparingMatcher.strictlyLowerThan("2026-01-01")).build();
        ISliceFilter after  = ColumnFilter.builder().column("date")
                .matching(ComparingMatcher.greaterThanOrEqual("2026-01-01")).build();
        return List.of(
            CubeQueryStep.edit(step).measure("d_legacy")
                    .filter(FilterBuilder.and(step.getFilter(), before).optimize()).build(),
            CubeQueryStep.edit(step).measure("d_modern")
                    .filter(FilterBuilder.and(step.getFilter(), after).optimize()).build()
        );
    })
    .build();
```

Source: [`RoutingMeasure`](https://github.com/solven-eu/adhoc/blob/master/engine/recipes/src/main/java/eu/solven/adhoc/measure/routing/RoutingMeasure.java),
[`IRoutingLogic`](https://github.com/solven-eu/adhoc/blob/master/engine/recipes/src/main/java/eu/solven/adhoc/measure/routing/IRoutingLogic.java),
[`RoutingMeasureQueryStep`](https://github.com/solven-eu/adhoc/blob/master/engine/recipes/src/main/java/eu/solven/adhoc/measure/routing/RoutingMeasureQueryStep.java).

---

## The two-class split

Every custom measure that uses underlyings is two classes:

|         File         |                                                Role                                                 |
|----------------------|-----------------------------------------------------------------------------------------------------|
| `RoutingMeasure`     | The **spec** (data class). Implements `IMeasure + IHasUnderlyingMeasures`. Held in the forest.      |
| `RoutingMeasureQueryStep` | The **runtime** (logic). Extends `AMeasureQueryStep`. Built per query step.                    |

The engine pairs them via `IMeasure.queryStepClass()`. The default convention
`<measure>` ↔ `<measure>QueryStep` works only when the step lives at
`eu.solven.adhoc.measure.transformator.step.<X>QueryStep`. Anything outside that package must
override:

```java
@Override
public String queryStepClass() {
    return RoutingMeasureQueryStep.class.getName();
}
```

`RoutingMeasure` overrides for both reasons: package mismatch and refactoring safety
(IDE renames the override; the string-formatted convention wouldn't catch it until the next
query at runtime).

---

## Trick 1 — Jackson and the `IRoutingLogic`

`RoutingMeasure` carries an `IRoutingLogic` (a `@FunctionalInterface`, so a lambda is enough at
the call site). **Lambdas / interface references are not JSON-serializable.** The class is
`@Jacksonized`, so the *other* fields round-trip cleanly:

```java
@JsonIgnore
@Nullable
IRoutingLogic routingLogic;
```

Three consequences:

1. **The forest cannot be declared in YAML/JSON today.** `MeasureForestFromResource` will parse
   every other field but `routingLogic` deserializes to `null`. A `RoutingMeasure` with
   `routingLogic == null` is *spec-only* — usable for introspection (forest summaries,
   dependency-graph generation, the explain output) but not for query execution
   (`RoutingMeasureQueryStep` throws on null).
2. **`@JsonIgnore` is not optional.** Without it, Jackson would fail on the lambda. With it,
   serialization is partial-by-design.
3. **Equality must exclude `routingLogic`** — otherwise a deserialized spec-only instance
   wouldn't equal the original. The class uses `@EqualsAndHashCode(exclude = "routingLogic")`
   so Jackson roundtrip preserves equality for the introspection use case.

The other examples in the codebase that need pluggable logic (e.g. `Combinator`,
`Dispatchor`) avoid this trap by identifying their logic with a string `combinationKey` /
`decompositionKey` resolved through `IOperatorFactory`. That keeps them YAML-declarable but
forces the logic class to be discoverable by name. The `routingOptions` field on
`RoutingMeasure` is reserved for a future declarative path: when an `IOperatorFactory` method
exists to resolve an `IRoutingLogic` implementation from a class FQCN plus an options map,
`routingOptions` will be passed to that implementation's `Map<String, ?>`-arg constructor.
The plumbing isn't there yet.

---

## Trick 2 — `underlyings` is a covering set, not a runtime constraint pretending to be informational

```java
@Singular
ImmutableList<String> underlyings;
```

`underlyings` declares the closed set of measure names the `routingLogic` may reference. It
serves two distinct audiences:

- **Static tooling** — `MeasureForest` summaries, dependency-graph generators, the explain
  output. None of these can run the routing logic to discover its targets, so they read this
  field to render the measure's dependencies.
- **Runtime validation** — `RoutingMeasureQueryStep` checks every step the logic returns
  against this set, and throws if a returned step targets a measure not listed. This makes
  the field load-bearing rather than informational, which keeps documentation and runtime
  behaviour in sync.

The error message names the bad return, the closed list, and the parent step:

```
RoutingMeasure 'dRouted': routingLogic returned a step targeting measure 'd_modern',
not in declared underlyings [d_legacy]. step=...
```

If you find yourself wanting to bypass the closed set ("the routing logic is dynamic and
depends on the query"), that's a hint to revisit the design — the logic isn't really routing,
it's something that needs a different measure type.

---

## Trick 3 — Coalesce, not SUM

The cross-step combine in `RoutingMeasureQueryStep` is hardcoded to `CoalesceCombination`
(first-non-null wins), not `SumCombination`. The contract is "one slice ↔ one sub-step":

- When the routing column is in the groupBy (or the user's filter restricts to one side),
  exactly one sub-step produces a value for any given output slice. The other side returns
  `null`. Coalesce passes the value through structurally — no arithmetic, no rounding, no
  type promotion.
- When filters happen to overlap, SUM would silently double-count. Coalesce surfaces the
  overlap as a "first wins" anomaly that's easier to spot. Routing isn't an aggregation
  operation — there is no "right way" to combine two values for the same logical cell, so
  refusing to merge is the safer default.

A short-circuit makes this efficient: with a single returned step, `produceOutputColumn`
returns the underlying cuboid as-is rather than rebuilding through coalesce. Mirrors
`CombinatorQueryStep`'s identical short-circuit.

---

## Trick 4 — `getUnderlyingNames` vs `getUnderlyingSteps`: the closed set vs the runtime decomposition

These two methods look similar and are easy to confuse:

```java
// Spec side — the closed set of measure names the logic MAY reference.
@JsonIgnore
@Override
public List<String> getUnderlyingNames() {
    return underlyings;          // closed list, declared at build time
}

// Step side — the actual queries the engine must run for THIS step.
@Override
public List<CubeQueryStep> getUnderlyingSteps() {
    List<CubeQueryStep> steps = measure.getRoutingLogic().route(step);
    // ... validation: each step's measure ∈ underlyings ...
    return steps;
}
```

The engine uses `getUnderlyingNames` at **planning time** to know which measures the DAG
must contain. It uses `getUnderlyingSteps` at **evaluation time** to know which subqueries to
issue.

For `RoutingMeasure` the cardinalities can differ: 2 names declared, 1 or 2 steps issued —
depending on whether the routing logic returns a single-step (passthrough) or multi-step
(decomposition) result. The engine doesn't require them to match.

---

## Trick 5 — `produceOutputColumn` and the column-factory pattern

`RoutingMeasureQueryStep` builds its output column via `IColumnFactory`, not by directly
constructing a `MultitypeHashColumn`:

```java
IMultitypeColumnFastGet<ISlice> values = factories.getColumnFactory()
        .makeColumn(p -> p.initialCapacity(IColumnFactory.sumSizes(underlyings)));
```

Two reasons not to call the column constructor directly:

1. **`IColumnFactory` may return a partitioned column** when the underlyings are partitioned,
   without the QueryStep needing to know. Hand-rolling `MultitypeHashColumn.builder()` opts
   the measure out of that optimization.
2. **Capacity sizing.** `IColumnFactory.sumSizes(underlyings)` gives a sensible upper bound
   for capacity, avoiding rehashing during the slice walk. Manually-built columns default to
   the (small) standard initial capacity.

The output is then attached to *our* step via `Cuboid.forGroupBy(step).values(values).build()`
— never `return underlyings.get(0)`, because the underlying cuboid is tied to the *underlying
step's* identity. The single-underlying coalesce short-circuit is the one exception, and it's
safe specifically because coalesce's output equals the underlying's output verbatim.

---

## Trick 6 — Debug logging via `ProxyValueReceiver`

`onSlice` writes to an `IValueReceiver` directly rather than going through
`ICombination.combine(slice, list) → Object`. The receiver-based combine is the non-deprecated
form, but it doesn't *return* a value — which makes a `[DEBUG]` log line ("we wrote X for
slice Y") awkward to construct.

The trick: wrap the receiver in a `ProxyValueReceiver` that captures whatever the combine
writes, log it, then pass it through. Mirrors `CombinatorQueryStep#combine`. The two copies
of this pattern should probably move to a shared helper on `AMeasureQueryStep`; until that
refactor lands, treat the two sites as paired.

---

## Trick 7 — How the DAG sees a routing measure

Routing creates an interesting DAG shape: the measure declares N underlyings (so the planner
plans for all N) and the runtime issues 1+ of them. Walking through a query like
`cube.execute(measure="dRouted", filter="country=FR")` with the date-cutoff routing above:

```text
Cube DAG (planning, before routingLogic runs):
    dRouted ───► d_legacy
            └──► d_modern    ◄── declared closed set (covering)

Cube DAG (after the step is built):
    dRouted ───► d_legacy filter=country=FR AND date<2026-01-01
            └──► d_modern filter=country=FR AND date>=2026-01-01
```

This is fine. The Cube DAG is a *plan*; only the steps an upstream actually requires are
executed, and identical steps coming from different measures are deduplicated. Routing
benefits from that deduplication for free as long as `getUnderlyingSteps()` builds steps via
`CubeQueryStep.edit(step).measure(...).filter(...).build()` (which preserves identity for
the unchanged fields).

For background on the two-DAG model and how custom measures fit, see
[CubeQueryEngine](cube-query-engine.md) and [Foundations § DAGs](foundations.md#dags-and-the-measure-tree).

---

## Trick 8 — Cross-boundary correctness is the caller's problem

The framework cannot detect:

- Whether returned steps' filters are disjoint.
- Whether the underlying aggregator is linear (SUM, COUNT) or not (MAX, RANK, percentile).
- Whether the routing column is in the groupBy.

Coalesce is correct when *exactly one* sub-step contributes a value per output slice — i.e.
when filters are disjoint AND (the routing column is in the groupBy OR the user's filter
already restricts to one side). When that doesn't hold, the result is order-dependent or
silently wrong.

Two coping strategies, neither of which the framework imposes:

- **Caller-side**: ensure the routing column is always in the groupBy when querying across
  the cutover. The standard recommendation. The DAG-level test
  `testGroupByDate_oneSidePerOutputSlice` shows this scenario.
- **Logic-side**: have `routingLogic.route(step)` throw when it detects the cross-boundary
  case. The exception surfaces as a step failure — louder than a wrong number.

A future iteration could replace the simple SUM-vs-coalesce knob with a
decomposition-aware combine that exploits filter disjointness statically, but the current
shape was deliberately chosen not to pretend to solve this case. See
[Foundations § Linearity](foundations.md#linearity-and-why-sum-is-special) for the underlying
reason.

---

## Trick 9 — Tests live alongside, not below

`engine/recipes/src/test/java/eu/solven/adhoc/measure/routing/` holds:

- `TestRoutingMeasure` — lightweight unit tests on the spec class only: builder defaults,
  `toString`, Jackson roundtrip. Does NOT extend the cube fixture; this is the place for
  tests that don't need an in-memory cube to mean something.
- `TestDag_RoutingMeasure` — DAG-level integration: registers the measure on a fixture cube
  and asserts end-to-end results across the date cutover, mirroring
  `TestDagAggregations_RatioByCombinator`.

Don't split a class into two just because the test count is high; split only when the second
class can drop the cube fixture entirely. `TestRoutingMeasure` is split off precisely because
it tests serialization and builder behaviour that have nothing to do with running queries.

---

## See also

- [Custom Measures](custom-measure.md) — the abstract pattern guide for `IHasUnderlyingMeasures` + `AMeasureQueryStep`.
- [Foundations](foundations.md) — DAGs, Boolean algebra of filters, OLAP basics, and why aggregator linearity matters.
- [ICombination](combination.md) — when a pluggable function is enough and you don't need a custom QueryStep.
- [Operators Factory](operators-factory.md) — how `combinationKey` strings get resolved into `ICombination` instances.
- [CubeQueryEngine](cube-query-engine.md) — two-DAG workflow (Cube DAG of measure logic, Table DAG of database queries).
