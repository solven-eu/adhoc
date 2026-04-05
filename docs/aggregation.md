# Custom Aggregations

An `IAggregation` defines how two partial results are combined into a single value. It is the building
block of every `Aggregator` measure — the leaves of the measure DAG that query the underlying table
directly.

```java
@FunctionalInterface
public interface IAggregation {
    Object aggregate(Object left, Object right);
}
```

The engine reduces an arbitrary number of rows by applying `aggregate` recursively:
`aggregate(aggregate(row1, row2), row3)`, and so on. Implementations must therefore be **associative**.

---

## Built-in aggregations

| Key | Class | Behaviour |
|-----|-------|-----------|
| `SUM` | `SumAggregation` | Numeric sum; promotes `int`→`long`, `float`→`double` |
| `COUNT` | `CountAggregation` | Counts non-null values |
| `AVG` | `AvgAggregation` | Arithmetic mean |
| `max` | `MaxAggregation` | Maximum (comparable) |
| `min` | `MinAggregation` | Minimum (comparable) |
| `PRODUCT` | `ProductAggregation` | Numeric product |
| `COALESCE` | `CoalesceAggregation` | Returns the first non-null value |
| `SUM_ELSE_SET` | `SumElseSetAggregation` | Sums numbers; collects non-numeric values in a `Set` instead of failing |

Use these by name as the `aggregationKey` of an `Aggregator`:

```java
Aggregator.builder()
        .name("revenue")
        .aggregationKey(SumAggregation.KEY)   // "SUM"
        .build()
```

---

## Custom aggregations: the MarketRiskSensitivity example

Standard numeric aggregations are not enough when the values being aggregated are domain objects with
their own merging logic. A common example in financial risk is **market risk sensitivities** —
structured objects that map multidimensional risk buckets (tenor × maturity) to delta values.

### The domain object

```java
@Value
@Builder
public class MarketRiskSensitivity {

    /** Maps coordinate sets (e.g. {tenor=1Y, maturity=2Y}) to their delta. */
    @Default
    Object2DoubleMap<Map<String, ?>> coordinatesToDelta = Object2DoubleMaps.emptyMap();

    public static MarketRiskSensitivity empty() { ... }

    public MarketRiskSensitivity addDelta(Map<String, ?> coordinates, double delta) { ... }

    /** Combines two sensitivities by summing deltas for matching coordinates. */
    public MarketRiskSensitivity mergeWith(MarketRiskSensitivity other) { ... }
}
```

A single row might carry:

```java
MarketRiskSensitivity.empty()
    .addDelta(Map.of("tenor", "1Y", "maturity", "2Y"), 12.34)
```

### The aggregation

```java
public class MarketRiskSensitivityAggregation implements IAggregation {

    @Override
    public Object aggregate(Object left, Object right) {
        if (left == null)  return right;
        if (right == null) return left;
        return ((MarketRiskSensitivity) left).mergeWith((MarketRiskSensitivity) right);
    }
}
```

The null-guards make aggregation safe when some rows are missing a sensitivity. The core logic
delegates to `mergeWith()`, an instance method on the domain object itself — a clean way to keep
aggregation logic co-located with the type that owns the merging semantics.

### Registering the aggregator

Reference the implementation by its fully-qualified class name:

```java
Aggregator.builder()
        .name("sensitivities")
        .columnName("sensitivities")
        .aggregationKey(MarketRiskSensitivityAggregation.class.getName())
        .build()
```

The full end-to-end example — including group-by tenor, filter by maturity, and explain output — is
demonstrated in `TestPartitionor_PnLExplain`.

---

## How the engine resolves a custom `aggregationKey`

`StandardOperatorFactory.makeAggregation()` handles the lookup:

1. **Built-in keys** (`"SUM"`, `"COUNT"`, …) are matched by a `switch` statement.
2. **Everything else** is treated as a fully-qualified class name and instantiated via reflection:
   - If the class has a `Map<String, ?>` constructor, it is called with `aggregationOptions`.
   - Otherwise the no-arg constructor is used.

This means any `IAggregation` implementation on the classpath can be referenced without registering
it anywhere — just pass its class name as the `aggregationKey`.

To share a custom key string (rather than repeating the class name), define a constant:

```java
public class MarketRiskSensitivityAggregation implements IAggregation {
    public static final String KEY = MarketRiskSensitivityAggregation.class.getName();
    // or a shorter alias registered in a custom IOperatorsFactory
}
```

---

## Aggregation logic placement: static vs instance method

Two natural patterns exist for where to put the merge logic:

| Pattern | Example | When to use |
|---------|---------|-------------|
| **Instance method on the domain object** | `left.mergeWith(right)` | When the domain object owns its algebra and is under your control |
| **Static method** | `Sensitivity.merge(left, right)` | When the domain object is a third-party type you cannot modify |

Both produce the same `IAggregation` implementation — the difference is only where the logic lives.

---

## Further reading

- `TestPartitionor_PnLExplain` — full scenario with `MarketRiskSensitivity` aggregated across colors and risk buckets
- `TestTableQuery_DuckDb_customAggregation` — custom aggregation over DuckDB-backed data
- [Operators Factory](operators-factory.md) — registering a custom key alias via `IOperatorsFactory`
- [Partitionor](partitionor.md) — using a custom aggregation as the re-aggregation step after per-partition combination
