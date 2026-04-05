# Partitionor

A `Partitionor` is a measure that evaluates its underlying measures at a **finer granularity** than the current query, applies a per-partition combination, and then **re-aggregates** the results back up to the query granularity.

This two-phase structure — widen → combine → aggregate — is the building block for any computation that depends on a dimension that the end-user does not group by.

---

## Motivation: Foreign-Exchange conversion

Consider a trading book where every position has an `amount` stored in its native currency (`ccyFrom`). A user queries `SUM(amount)` grouped by `desk`, expecting a single number in a base currency (e.g. USD).

A plain `SUM` aggregator cannot produce this: it would add EUR amounts to GBP amounts to USD amounts without converting. The conversion rate is a function of `ccyFrom`, so the engine must know the currency of each partial sum before it can apply the rate.

A `Partitionor` solves this cleanly:

1. **Widen the groupBy** — evaluate `SUM(amount)` grouped by `(desk, ccyFrom)`.
2. **Combine per partition** — for each `(desk, ccyFrom)` cell, multiply the amount by the FX rate for that currency.
3. **Aggregate** — sum the converted amounts across currencies, collapsing back to `(desk)`.

The user still queries by `desk`; the intermediate currency dimension is invisible.

---

## How the GROUP BY widens

Given a parent node with `groupBy = (desk)` and a `Partitionor` with its own `groupBy = (ccyFrom)`, the engine queries the underlying measure at the **union**:

```
underlying groupBy = union( (desk), (ccyFrom) ) = (desk, ccyFrom)
```

The union is computed in `PartitionorQueryStep.getUnderlyingGroupBy()`:

```java
return GroupByHelpers.union(step.getGroupBy(), partitionor.getGroupBy());
```

Once the underlying result is available at `(desk, ccyFrom)`, the combination function (`ForeignExchangeCombination`) is called once per distinct `(desk, ccyFrom)` cell. The aggregation function (`SumAggregation`) then folds those converted values back to `(desk)`.

---

## Builder syntax

```java
Partitionor.builder()
    .name("k1.CCY")
    .underlyings(List.of("k1"))                         // measure(s) to evaluate per partition
    .groupBy(GroupByColumns.named("ccyFrom"))            // the partitioning dimension
    .combinationKey(ForeignExchangeCombination.KEY)      // combine per (desk, ccyFrom) cell
    .aggregationKey(SumElseSetAggregation.KEY)           // aggregate across currencies
    .build()
```

| Field | Purpose |
|---|---|
| `underlyings` | Measures evaluated at the widened groupBy |
| `groupBy` | Extra columns added to the parent groupBy for the underlying query |
| `combinationKey` | `ICombination` applied to each partition cell; receives the slice (including currency) |
| `aggregationKey` | `IAggregation` used to fold partition results back to parent granularity |

---

## Implementing the combination: ForeignExchangeCombination

The combination function receives the full slice — including `ccyFrom` — and the underlying value. It can therefore look up the FX rate and return the converted amount:

```java
public class ForeignExchangeCombination implements ICombination {

    public static final String KEY = "FX";

    @Override
    public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
        Object rawAmount = underlyingValues.get(0);
        if (rawAmount == null) return null;

        String ccyFrom = (String) slice.getRaw("ccyFrom");
        double rate = fxStorage.getRate(ccyFrom, baseCurrency);

        return ((Number) rawAmount).doubleValue() * rate;
    }
}
```

The full example, including an `IForeignExchangeStorage` backed by a `Map<(ccyFrom, ccyTo), Double>`, is demonstrated in `TestCubeQueryFx`.

---

## Comparison with other measure types

| | Combinator | Filtrator | Partitionor |
|---|---|---|---|
| **Changes groupBy?** | No | No | Yes — widens to include partition columns |
| **Changes filter?** | No | Yes — ANDs an extra filter | No |
| **Evaluation phases** | Single | Single | Two (combine per partition, then aggregate) |
| **Typical use** | Ratios, expressions | Scoped sub-totals | FX conversion, any rate × amount pattern |

---

## Further reading

- `TestCubeQueryFx` — end-to-end FX conversion with missing-rate handling
- `TestTransformator_Partitionor` — "sum of max" pattern (max per group, sum across groups)
- `ForeignExchangeCombination` — reference `ICombination` implementation for currency conversion
- [Concepts → Measure archetypes](concepts.md) — overview of all measure types
