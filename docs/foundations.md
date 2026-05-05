# Foundations

Adhoc reuses ideas from several mature areas of computer science. You can use it without ever
naming them, but knowing which concept underpins which API makes the surface dramatically more
predictable. This page is the short reference: the four families of ideas you will see again
and again across the codebase, and where in the source they actually surface.

This page does **not** restate the API. For that, see [Concepts](concepts.md) and
[Lexicon](lexicon.md). The point here is the *why* behind the design — the assumptions
custom-measure authors must keep in mind, and the failure modes that arise when those
assumptions don't hold for a particular use case.

---

## OLAP basics

**Online Analytical Processing.** A class of read-only queries that compute aggregated
indicators over multidimensional data. The mental model:

- **Dimensions** (a.k.a. **columns**, or in some literature **axes**): categorical
  attributes used to slice the data — e.g. `country`, `city`, `asOf`. In Adhoc these are
  represented by `IAdhocColumn`.
- **Coordinates**: a value picked along a dimension — `country=FR`, `asOf=2026-05-04`.
- **Slice**: a tuple of coordinates, one per dimension in scope — `{country=FR, city=Paris}`.
  Implemented in Adhoc as `ISlice`, backed by `IAdhocMap`. See [Slice and IAdhocMap](slice.md).
- **Measure**: a numeric (or domain-typed) indicator computed *per slice* — e.g.
  `revenue = SUM(amount)`. In Adhoc these implement `IMeasure`.
- **Cube**: the conceptual multidimensional array indexed by slices, with one or more
  measures at each cell. In Adhoc, an `ICubeWrapper` pairs a table (the data) with a
  measure forest (the indicator definitions).

A query, in this model, is a `(filter, groupBy, measures)` triple: which slices of the cube
should be returned, grouped at what granularity, and which measure values to compute on each.

### Why it matters for custom-measure authors

When writing a custom measure, the key decision is: **for each output slice, what do I need
from underlying measures?** The answer falls in one of three categories:

1. **Same slice, transformed value.** The output value at slice S is some function of the
   underlying value(s) at slice S. This is the `Combinator` shape — see
   [ICombination](combination.md).
2. **Different slice(s), values composed.** The output value at slice S depends on values
   at *other* slices — typically S with a coordinate edited (`Shiftor`), or a wider slice
   that S is a part of (`Unfiltrator`), or a narrower partition (`Partitionor`).
3. **Routing/dispatch.** The output value at slice S is the value of *one of N* underlyings
   at S, with the choice depending on S itself. See [Routing Measure](routing-measure.md).

Each shape maps cleanly onto a built-in archetype or a custom measure. The wrong shape
costs you in `getUnderlyingSteps()` complexity.

---

## DAGs and the measure tree

Adhoc evaluates queries as a **directed acyclic graph** (DAG) of `CubeQueryStep`s. Every
node is a `(measure, filter, groupBy, options)` tuple — the smallest unit of work the engine
can plan, cache, or skip. Edges go from a measure to its underlyings.

```text
user query: pnl.routed @ groupBy=desk @ filter=date >= 2026-01-01
					│
					▼
			┌────────────────┐
			│ pnl.routed     │   ◄─ root step (the user's measure)
			└────────┬───────┘
					│  getUnderlyingSteps()
			┌──────────┴──────────┐
			▼                     ▼
┌────────────────┐    ┌────────────────┐
│ pnl.legacy     │    │ pnl.new        │   ◄─ underlying steps
│ filter=before  │    │ filter=after   │      (filters narrowed
└────────┬───────┘    └────────┬───────┘      by the routing logic)
			│                     │
			▼                     ▼
┌────────────────┐    ┌────────────────┐
│ Aggregator     │    │ Aggregator     │   ◄─ leaves: actual table queries
└────────────────┘    └────────────────┘
```

The DAG is built in two passes — the **Cube DAG** (measure logic) and the **Table DAG**
(database queries). See [CubeQueryEngine](cube-query-engine.md) for the wiring; for the
purposes of this page, what matters is:

### Why DAG, not tree

Two distinct measures can share the same underlying step. If `pnl.fr_share = pnl.fr / pnl.total`
and `pnl.us_share = pnl.us / pnl.total`, the `pnl.total` step is a *single* node referenced
twice — computed once, consumed by both. The "same `(measure, filter, groupBy, options)`"
identity rule is what makes this deduplication possible. Custom measures benefit from it
for free as long as their `getUnderlyingSteps()` returns canonical, equality-comparable
steps (the standard `CubeQueryStep.edit(step).measure(...).build()` builder does this).

### Why acyclic

A measure that depended on itself would either loop forever or require a fixed-point
iteration, neither of which the engine offers. The forest's `addMeasure` rejects cycles at
registration time. If you need conditional self-reference (e.g. "use this measure unless we
already computed it for a parent slice"), the right pattern is `Shiftor` or a custom
measure that re-issues a step for a different slice — same measure, different `step`, no
cycle.

### Why "step", not "measure"

The same logical measure (e.g. `pnl.legacy`) can appear at multiple steps with different
filters and groupBys. Caching, induced-query optimization, and ratio-style measures (which
re-issue an underlying with a different filter — see
[`RatioByCombinator`](https://github.com/solven-eu/adhoc/blob/master/engine/cube/src/test/java/eu/solven/adhoc/measure/ratio/RatioByCombinator.java))
all rely on this. The DAG node is the step, not the measure.

### Why this matters for custom-measure authors

`getUnderlyingSteps()` is your declaration of edges in the DAG. Three rules:

1. **Build steps via `CubeQueryStep.edit(step)`**, not `CubeQueryStep.builder()` from
   scratch. The former preserves identity for fields you don't change (filter, groupBy,
   options) so the engine can dedupe across measures. The latter would create a "different"
   step that happens to be equal — which fails identity-based caching.
2. **Return a stable list per step.** The same step, called twice, must produce the same
   underlying steps. Random ordering or non-determinism breaks the DAG's idempotence and
   makes cached results non-reproducible.
3. **Underlyings declared in `getUnderlyingNames()` are a superset of what
   `getUnderlyingSteps()` may return.** The planner uses the names list to build the DAG's
   shape; the steps list is consulted at evaluation time. If your runtime steps reference a
   measure not in the names list, the DAG has no node for it and the engine fails.

---

## Boolean algebra of filters

`ISliceFilter` is a Boolean algebra over slices. A slice either matches a filter or
doesn't; filters compose with the standard operators:

| Adhoc construct |        Boolean meaning        |    Identity element     |
|-----------------|-------------------------------|-------------------------|
| `AndFilter`     | conjunction (AND)             | `MATCH_ALL` (empty AND) |
| `OrFilter`      | disjunction (OR)              | `MATCH_NONE` (empty OR) |
| `NotFilter`     | negation (NOT)                | —                       |
| `ColumnFilter`  | atom: predicate on one column | —                       |
| `MATCH_ALL`     | tautology (⊤)                 | unit of AND             |
| `MATCH_NONE`    | contradiction (⊥)             | unit of OR              |

The standard identities all hold: `AND(x, MATCH_ALL) ≡ x`, `OR(x, MATCH_NONE) ≡ x`,
`AND(x, MATCH_NONE) ≡ MATCH_NONE`, `OR(x, MATCH_ALL) ≡ MATCH_ALL`, `NOT(NOT(x)) ≡ x`,
De Morgan, etc. `FilterBuilder.optimize()` applies these rewrites and several others; do
not bypass it when constructing filters in custom measures.

### Why this matters for custom-measure authors

Custom measures that manipulate filters (e.g. routing, partitioning, shifting) routinely
construct ANDs of the user's filter with a measure-side filter:

```java
ISliceFilter narrowed = FilterBuilder.and(step.getFilter(), measureSideFilter).optimize();
```

`optimize()` is where:

- `MATCH_NONE` short-circuits propagate up — e.g. if the user filtered to `country=FR` and
  the measure-side filter is `country=US`, the AND collapses to `MATCH_NONE`. Catch this
  before issuing an underlying step (see `RatioByCombinatorQueryStep`'s pattern of
  detecting the empty branch).
- Equivalent filters become structurally equal, which lets the DAG dedupe steps that arose
  from different measure-construction paths.

Filters are also useful as values you can read inside `routeFunction` of a
[`RoutingMeasure`](routing-measure.md): `FilterHelpers.asMap(step.getFilter())` extracts an
AND-of-equalities into a `Map<String, Object>`, which is the simplest case to dispatch on.
For non-AND-of-equalities (ranges, OR, regex), you'll need to inspect the filter tree
directly via `FilterHelpers.visit(filter, visitor)`.

### Filters are *not* slices

A common confusion: a `ColumnFilter` carries a column name and a value matcher; an
`ISlice` carries column-to-coordinate bindings. They look similar — both attach values to
columns — but they answer different questions:

- A filter answers "does this slice belong to my set?"
- A slice answers "what are my coordinates?"

`FilterHelpers.asMap` exists because for the special case of AND-of-equalities, a filter is
*shaped like* a slice. Most of the time it isn't, and treating it as one in your custom
measure is the wrong abstraction — see [Filtering](filtering.md) for the full surface.

---

## Linearity and why SUM is special

An aggregation `f` is **linear** when, given any partition of the rows into disjoint
groups `A` and `B`, `f(A ∪ B) = f(A) + f(B)` for some merging operator `+`. SUM and COUNT
are linear — the merge operator is itself addition. Most other useful aggregations are not.

|      Aggregation       |           Linear?            |       Merge of disjoint partitions        |
|------------------------|------------------------------|-------------------------------------------|
| `SUM`                  | yes                          | `f(A∪B) = f(A) + f(B)`                    |
| `COUNT`                | yes                          | `f(A∪B) = f(A) + f(B)`                    |
| `AVG`                  | **no**                       | needs sum + count separately, then divide |
| `MIN` / `MAX`          | yes (idempotent merge)       | `min(f(A), f(B))` / `max(...)`            |
| `RANK` / `top-K`       | **no**                       | needs the raw values from both partitions |
| `MEDIAN`, `PERCENTILE` | **no**                       | needs the full distribution               |
| `STDDEV`               | **no** (without extra state) | needs sum of squares + sum + count        |

This is more than vocabulary. Several patterns in Adhoc — and several use cases users want
to write — work cleanly only when the aggregation is linear:

1. **Decomposition into disjoint sub-queries.** A custom measure that splits a query into
   two filters whose union is the original (e.g. `country=FR ∨ country=US`), runs them
   separately, and sums the results, is doing this *for free* with SUM. Trying the same
   trick with MAX of two disjoint partitions still works (because MAX is idempotent on the
   merge: `max(max(A), max(B)) = max(A∪B)`). Trying it with MEDIAN does not — there is no
   way to recover the median of `A∪B` from the medians of `A` and `B` alone.
2. **Caching at intermediate levels.** A linear aggregation can be cached at one
   granularity and rolled up to a coarser one by re-applying the merge. Non-linear
   aggregations cannot — caching them is only valid for the exact granularity they were
   computed at.
3. **Routing across boundaries.** [`RoutingMeasure`](routing-measure.md) explicitly does
   not support cross-boundary queries when the routing column isn't in the groupBy. The
   reason is precisely linearity: even if the underlying measure sums cleanly, the routing
   measure would have to combine values from *two different physical aggregations* —
   which is only safe when those aggregations are linear in the same merge sense.

### Why this matters for custom-measure authors

When you design a custom measure, ask:

- "Can I express my output as a function of underlying measure values *at the same slice*?"
  → Combinator-shaped, no linearity question, see [ICombination](combination.md).
- "Do I need to combine values from *different slices* into one output?" → Linearity
  matters. If your underlying is SUM you're fine; if it's MAX you need to think about
  whether merge-by-MAX gives the right answer; if it's MEDIAN, refuse the operation
  loudly rather than producing a silently-wrong number.
- "Does my measure need to *partition* the data and aggregate the partitions?" → That's
  Partitionor's job — see [Partitionor](partitionor.md). It works because the engine
  delegates the per-partition aggregation to the same `IAggregation` as the underlying,
  which preserves correctness as long as that aggregation is associative.

The single biggest correctness trap is to treat all aggregations as if they were SUM. SUM
is associative, commutative, has an identity (0), distributes over partitions, caches at
any granularity, and is its own merge operator. Almost no other aggregation has all these
properties. Whenever you see code that combines values across slices or partitions in a
custom measure, the question to ask is: "would this still be correct if the underlying
were RANK?" If the answer is no, document the constraint.

---

## See also

- [Concepts](concepts.md) — concrete architecture and query flow.
- [Lexicon](lexicon.md) — Adhoc-specific terms and ActivePivot/Atoti translations.
- [Filtering](filtering.md) — full surface of `ISliceFilter` and `IValueMatcher`.
- [Custom Aggregations](aggregation.md) — implementing `IAggregation` (associativity is the only hard requirement).
- [Custom Measures](custom-measure.md) — the abstract pattern guide.
- [Routing Measure](routing-measure.md) — concrete walkthrough that exercises every concept above.
- [CubeQueryEngine](cube-query-engine.md) — two-DAG workflow (Cube DAG of measures, Table DAG of database queries).

