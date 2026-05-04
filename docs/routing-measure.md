# RoutingMeasure — A Walkthrough

This page walks through the actual `RoutingMeasure` shipped in `engine/adhoc`, focusing on the
practical tricks you hit when writing a custom measure: where the runtime wiring lives, what
Jackson can and cannot serialize, how `getUnderlyingSteps` shapes the DAG, and what
`produceOutputColumn` is allowed to assume.

For the abstract pattern guide and the full contract surface, see
[Custom Measures](custom-measure.md). This page is the worked-example companion.

---

## What `RoutingMeasure` does

A `RoutingMeasure` dispatches each `CubeQueryStep` to one of several declared underlying
measures, picked by a caller-supplied `Function<CubeQueryStep, String>`. The typical use case
is migrations (e.g. Atoti → Adhoc) where one logical measure must map to different physical
measures depending on the slice — one underlying for data before a modelling cutoff, another
for data after it.

```java
RoutingMeasure.builder()
    .name("dRouted")
    .underlying("d")              // declared universe...
    .underlying("d_fr")           //   ...the routeFunction
    .underlying("d_us")           //   ...may pick from
    .routeFunction(step -> {
        Object country = FilterHelpers.asMap(step.getFilter()).get("country");
        if ("FR".equals(country)) return "d_fr";
        if ("US".equals(country)) return "d_us";
        return "d";
    })
    .build();
```

Source: [`RoutingMeasure`](https://github.com/solven-eu/adhoc/blob/master/engine/adhoc/src/main/java/eu/solven/adhoc/measure/routing/RoutingMeasure.java),
[`RoutingMeasureQueryStep`](https://github.com/solven-eu/adhoc/blob/master/engine/adhoc/src/main/java/eu/solven/adhoc/measure/routing/RoutingMeasureQueryStep.java).

---

## The two-class split

Every custom measure that uses underlyings is two classes:

|         File         |                                        Role                                        |
|----------------------|-------------------------------------------------------------------------------------|
| `RoutingMeasure`     | The **spec** (data class). Implements `ICombinator`. Held in the `MeasureForest`.   |
| `RoutingMeasureQueryStep` | The **runtime** (logic). Extends `AMeasureQueryStep`. Built per query step.    |

The engine pairs them via `IMeasure.queryStepClass()`. The default convention
`<measure>` ↔ `<measure>QueryStep` works as long as both classes live in packages the engine's
classloader can resolve — which is always true for code in your own module.

`RoutingMeasure` overrides `queryStepClass()` explicitly:

```java
@Override
public String queryStepClass() {
    return RoutingMeasureQueryStep.class.getName();
}
```

Two reasons to override rather than rely on the convention:

1. **Refactoring safety.** If someone renames the spec class, the IDE updates the override
   automatically; the convention's string-formatting trick wouldn't catch it until the next
   query at runtime.
2. **Custom packaging.** The convention assumes the step lives in
   `eu.solven.adhoc.measure.transformator.step.<X>QueryStep`. Custom measures placed outside
   that package must override.

---

## Trick 1 — Jackson and the `Function`

`RoutingMeasure` carries a `Function<CubeQueryStep, String>`. **Lambdas are not
JSON-serializable.** This has two consequences you must accept:

```java
@JsonIgnore
@NonNull
Function<CubeQueryStep, String> routeFunction;
```

1. **The forest cannot be declared in YAML/JSON.** `MeasureForestFromResource` will succeed
   parsing every other field but the `routeFunction` cannot be reconstructed. Any forest
   containing a `RoutingMeasure` must be registered programmatically.
2. **`@JsonIgnore` is not optional.** Without it, Jackson will fail to serialize the spec at
   all (the default ObjectMapper has no idea how to write a lambda). With it, the spec is
   round-trippable for the *other* fields — useful for explain/debug output that prints the
   measure tree without trying to reconstruct the function.

The other examples in the codebase that need pluggable logic (e.g. `Combinator`,
`Dispatchor`) avoid this trap by identifying their logic with a string `combinationKey` /
`decompositionKey` resolved through `IOperatorFactory`. That keeps them YAML-declarable but
forces the logic class to be discoverable by name — a different ergonomic trade-off.

---

## Trick 2 — Carrying `combinationKey` even when you don't combine

`ICombinator` extends `IHasCombinationKey`, which mandates a `combinationKey` and
`combinationOptions`. `RoutingMeasure` never actually invokes a combination — it just passes
the routed branch's value through unchanged. But the interface contract forces the field:

```java
@NonNull
@Builder.Default
String combinationKey = SumCombination.KEY;

@NonNull
@Builder.Default
Map<String, ?> combinationOptions = Collections.emptyMap();
```

Defaulting to `SumCombination.KEY` is not load-bearing; the value is never consulted. If the
field ever appears wrong in an explain trace, that's a hint someone is invoking the
combination machinery for routing, which they shouldn't be — see Trick 4.

If your custom measure has *no* sensible combination (e.g. its logic genuinely cannot be
expressed as combine-of-N-values), consider whether `ICombinator` is actually the right
super-interface. `IHasUnderlyingMeasures` (without combination) is the lower-level option,
at the cost of writing a bit more wiring yourself.

---

## Trick 3 — `getUnderlyingNames` vs `getUnderlyingSteps`: the closed set vs the runtime set

These two methods look similar and are easy to confuse:

```java
// Spec side — the universe of measures this measure may consume.
@JsonIgnore
@Override
public List<String> getUnderlyingNames() {
    return underlyings;          // closed list, declared at build time
}

// Step side — the actual queries the engine must run for THIS step.
@Override
public List<CubeQueryStep> getUnderlyingSteps() {
    String chosen = measure.getRouteFunction().apply(step);
    // ... validation ...
    return List.of(CubeQueryStep.edit(step).measure(chosen).build());
}
```

The engine uses `getUnderlyingNames` at **planning time** to know which measures the DAG
must contain. It uses `getUnderlyingSteps` at **evaluation time** to know which subqueries to
issue.

For `RoutingMeasure` they have different cardinalities: 3 names declared, exactly 1 step
issued. That asymmetry is fine — the engine doesn't require them to match.

**The validation matters.** If `routeFunction` returns `"d_xx"` (a name not in the closed
list), the engine has no way to compose the result back into the DAG: it never planned for
`d_xx`. Catching this in the step with a precise error is non-negotiable:

```java
if (!measure.getUnderlyings().contains(chosen)) {
    throw new IllegalStateException(
        "RoutingMeasure '%s': routeFunction returned '%s', not in declared underlyings %s. step=%s"
            .formatted(measure.getName(), chosen, measure.getUnderlyings(), step));
}
```

The exception message includes the measure name, the bad return, the closed list, and the
full step — everything a debugger needs without re-running with `--explain`.

---

## Trick 4 — `produceOutputColumn`: passthrough vs combination

`AMeasureQueryStep.produceOutputColumn(List<? extends ICuboid>)` is where the runtime turns
underlying results into the measure's own output. `RoutingMeasureQueryStep` does a
**passthrough** — for every slice in the (single) underlying cuboid, it emits the same value
attached to its own step:

```java
@Override
public ICuboid produceOutputColumn(List<? extends ICuboid> underlyings) {
    if (underlyings.size() != 1) {
        throw new IllegalArgumentException(/* ... */);
    }

    IMultitypeColumnFastGet<ISlice> values = makeStorage();
    ICombination passthroughCombination = factories.getOperatorFactory().makeCombination(measure);
    forEachDistinctSlice(underlyings, passthroughCombination, values::append);

    return Cuboid.forGroupBy(step).values(values).build();
}

@Override
protected void onSlice(SliceAndMeasures slice, ICombination combination, ISliceAndValueConsumer output) {
    Object value = slice.getMeasures().asList().get(0);
    output.putSlice(slice.getSlice().getSlice()).onObject(value);
}
```

Two practical things to notice:

1. **Don't return the underlying cuboid directly.** `underlyings.get(0)` is tied to the
   *underlying step's* identity, not ours. Downstream consumers (caching, explain traces,
   the DAG join logic) match cuboids by step, so the values must be re-attached to *our*
   step via `Cuboid.forGroupBy(step).values(...).build()`. A naive `return underlyings.get(0)`
   compiles, runs, and produces correct numbers — but breaks observability.

2. **`onSlice` ignores the `combination` parameter.** The `passthroughCombination` is required
   by `AMeasureQueryStep#forEachDistinctSlice`'s signature but never invoked by our `onSlice`.
   That's the price of reusing the existing slice-iteration scaffolding for a non-combining
   measure. The alternative — calling `joinCuboids(underlyings)` directly without going
   through `forEachDistinctSlice` — duplicates a lot of debug-logging and error-wrapping
   from the parent class for no real benefit.

---

## Trick 5 — How the DAG sees a routing measure

Routing creates an interesting DAG shape: the measure declares N underlyings (so the planner
plans for all N) but at runtime only 1 underlying is actually queried per step. Walking
through a query like `cube.execute(measure="dRouted", filter="country=FR")` with the routing
function above:

```text
Cube DAG (planning, before routeFunction runs):
    dRouted ───► d
            ├──► d_fr      ◄── declared dependencies (closed set)
            └──► d_us

Cube DAG (after the step is built and routeFunction returned "d_fr"):
    dRouted ───► d_fr      ◄── only the chosen branch is materialized
                              d and d_us are NOT queried
```

This is fine. The Cube DAG is a *plan*; only the steps that an upstream actually requires
are executed. `RoutingMeasureQueryStep#getUnderlyingSteps` returning a single `CubeQueryStep`
means the planner only allocates a query for that one branch.

What this **doesn't** do: it doesn't free your routing function from the responsibility of
picking the right branch. The DAG won't second-guess your function's choice — if the query
filter was `country=FR` but you returned `d_us`, the engine will dutifully query `d_us` and
return its (probably wrong) value. Whatever invariants the routing represents must be
enforced inside `routeFunction`.

For background on the two-DAG model and how custom measures fit, see
[CubeQueryEngine](cube-query-engine.md) and [Foundations § DAGs](foundations.md#dags-and-the-measure-tree).

---

## Trick 6 — Cross-boundary queries are not supported

The contract is "one underlying per step". A query whose filter spans both sides of the
routing boundary *without* the routing column being part of the groupBy presents a problem
the framework cannot solve from inside this measure: there is no single underlying that holds
the right value for the whole slice.

`RoutingMeasure` does not detect this case. The function is opaque — the engine cannot tell
whether `routeFunction.apply(step)` is "correct" for a step that straddles the boundary.
Two coping strategies:

- **Caller-side**: ensure the routing column is always in the groupBy when querying across
  the cutover, so each output slice maps to exactly one branch. This is the standard
  recommendation.
- **Function-side**: have `routeFunction` throw with a precise message when it detects the
  cross-boundary case, refusing to dispatch silently to one side. The exception will surface
  in the DAG as a step failure, which is louder than a wrong number.

A future iteration could replace `Function<CubeQueryStep, String>` with a decomposition
strategy that returns multiple `(filter, underlying)` pairs covering the step's filter
disjointly — at which point `produceOutputColumn` would need to combine the results, and
the linearity question becomes load-bearing (see
[Foundations § Linearity](foundations.md#linearity-and-why-sum-is-special)). The current
shape was deliberately chosen to not pretend to solve this case.

---

## Trick 7 — Tests live alongside, not below

`engine/adhoc/src/test/java/eu/solven/adhoc/measure/routing/TestDag_RoutingMeasure.java`
holds the full coverage in a single class — routing decisions, error cases (unknown
underlying, null underlying), and DAG fan-out scenarios where the routing measure is
queried alongside its raw underlyings. It mirrors the precedent of
`TestDagAggregations_RatioByCombinator`: one class, all tests against the same in-memory
fixture.

The fixture base class `ATestDagInMemory` (from `engine/cube/src/test/java/...`, available
via the `tests` classifier) gives you a `forest`, a `table()` and a `cube()` ready to use.
DAG-level tests catch both logic bugs (a wrong routing decision shows up as a wrong number)
and wiring bugs (e.g. forgetting `Cuboid.forGroupBy` in `produceOutputColumn` shows up as
caching / explain breakage).

A separate pure-unit test class would only be worthwhile if the spec or step had logic
that's awkward to exercise via a cube — which is not the case here. Don't split a class
into two just because the test count is high; split only when the second class can drop
the cube fixture entirely.

---

## See also

- [Custom Measures](custom-measure.md) — the abstract pattern guide for `IHasUnderlyingMeasures` + `AMeasureQueryStep`.
- [Foundations](foundations.md) — DAGs, Boolean algebra of filters, OLAP basics, and why aggregator linearity matters.
- [ICombination](combination.md) — when a pluggable function is enough and you don't need a custom QueryStep.
- [Operators Factory](operators-factory.md) — how `combinationKey` strings get resolved into `ICombination` instances.
- [CubeQueryEngine](cube-query-engine.md) — two-DAG workflow (Cube DAG of measure logic, Table DAG of database queries).
