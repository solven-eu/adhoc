# ICombination

`ICombination` is the simplest extension point for injecting business logic into the measure
evaluation pipeline. It receives the values of one or more underlying measures at a single slice
and returns a new value.

It is simpler than a full [custom measure](custom-measure.md) because the engine handles all
the plumbing — iterating slices, routing underlying queries, assembling the output cuboid. The
`ICombination` only sees one slice at a time.

---

## Where ICombination is used

| Measure type  |                                                                   How the combination is applied                                                                   |
|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Combinator`  | Called once per output slice; receives the underlying measure values for that slice                                                                                |
| `Partitionor` | Called once per *partition slice* (the induced GROUP BY); receives the underlying values at that finer granularity, before the re-aggregation step folds them back |

In both cases the signature is the same:

```java
@FunctionalInterface
public interface ICombination {

	/** Combine underlying values at a single slice into one output value. */
	Object combine(ISliceWithStep slice, List<?> underlyingValues);

	/** Alternative overload using ISlicedRecord for indexed access. */
	default IValueProvider combine(ISliceWithStep slice, ISlicedRecord slicedRecord) { ... }
}
```

`underlyingValues` contains one entry per underlying measure, in the same order as declared in
the measure's `underlyings` list. Entries can be `null` when a given underlying produced no value
for that slice.

---

## Accessing the current slice: ISliceWithStep

The `slice` parameter provides full context about the current coordinate. The most important path
is `sliceReader()`, which offers a type-safe API for extracting column values:

```java
ISliceReader reader = slice.sliceReader();
```

### Extracting a column value

```java
// Typed extraction — throws if the column is absent or holds a null
String ccy = reader.extractCoordinate("ccy", String.class);

// Lax extraction — returns Optional.empty() if the column is absent
Optional<String> maybeCcy = reader.extractCoordinateLax("ccy", String.class);

// Raw matcher — returns the IValueMatcher stored for this column
IValueMatcher matcher = reader.getValueMatcher("ccy");
```

`extractCoordinate` is the right choice when the column is guaranteed to be present and non-null.
For nullable or optional columns, prefer `extractCoordinateLax` or inspect the raw `IValueMatcher`.

### The GROUP BY guarantee

When the engine iterates slices to call a `Combinator` or `Partitionor` combination, each slice
represents one row of the GROUP BY result. Every column in the GROUP BY is therefore **fully
determined** for that slice — its value is either a concrete value or an explicit null. This
translates directly to the `ISliceReader`:

- A non-null GROUP BY coordinate → `EqualsMatcher` → `extractCoordinate()` returns the value.
- A null GROUP BY coordinate → `NullMatcher` → `extractCoordinate()` returns `null`; the matcher
  is a `NullMatcher`, not an `EqualsMatcher`.

When writing a combination that reads GROUP BY columns, handle the null case explicitly:

```java
IValueMatcher matcher = reader.getValueMatcher("ccy");
if (matcher instanceof EqualsMatcher eq) {
	String ccy = (String) eq.getOperand();
	// ... use ccy
} else if (matcher instanceof NullMatcher) {
	// ccy is null for this slice — decide how to handle it
}
```

Or with `extractCoordinateLax`:

```java
Optional<String> ccy = reader.extractCoordinateLax("ccy", String.class);
ccy.ifPresentOrElse(
	c -> /* use c */,
	() -> /* handle null-ccy slice */);
```

---

## Example: ForeignExchangeCombination

A `Combinator` groups by `ccy` and applies FX conversion per currency. The combination reads the
`ccy` coordinate from the slice and looks up the rate:

```java
public class ForeignExchangeCombination implements ICombination {

	public static final String KEY = "FX";

	private final IForeignExchangeStorage fxStorage;
	private final String baseCcy;

	@Override
	public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		Object rawAmount = underlyingValues.get(0);
		if (rawAmount == null) return null;

		// The Partitionor groups by "ccy", so this coordinate is always present
		String ccy = slice.sliceReader().extractCoordinate("ccy", String.class);

		double rate = fxStorage.getRate(ccy, baseCcy);
		return ((Number) rawAmount).doubleValue() * rate;
	}
}
```

Used inside a `Partitionor`:

```java
Partitionor.builder()
		.name("pnl.usd")
		.underlyings(List.of("pnl"))
		.groupBy(GroupByColumns.named("ccy"))          // guarantees ccy is in the slice
		.combinationKey(ForeignExchangeCombination.KEY)
		.aggregationKey(SumAggregation.KEY)
		.build()
```

Because `ccy` is in the `Partitionor`'s `groupBy`, `extractCoordinate("ccy", String.class)` is
always safe — except when a row has a null `ccy` value (see NullMatcher note above).

---

## Registering a custom ICombination

Reference the implementation class name as the `combinationKey`:

```java
Combinator.builder()
		.name("pnl.converted")
		.underlyings(List.of("pnl"))
		.combinationKey(ForeignExchangeCombination.class.getName())
		.build()
```

`StandardOperatorFactory` will instantiate it via reflection (no-arg constructor, or
`Map<String, ?>` constructor if `combinationOptions` are provided). To inject Spring beans or
other collaborators, use a custom `IOperatorsFactory` — see [Operators Factory](operators-factory.md).

---

## ISlicedRecord vs List

`ICombination.combine` has two overloads:

|                Overload                |                                                  When to use                                                  |
|----------------------------------------|---------------------------------------------------------------------------------------------------------------|
| `combine(slice, List<?> values)`       | Simple cases; values accessed by index                                                                        |
| `combine(slice, ISlicedRecord record)` | When indexed access via `record.read(int)` returning `IValueProvider` is preferred over boxing through `List` |

Both overloads see the same data. `ISlicedRecord` avoids materialising all values into a `List`
when only one or two are needed, reducing allocation on the hot path.

---

## Further reading

- [Partitionor](partitionor.md) — how `ICombination` is used for per-partition logic before re-aggregation
- [Custom Aggregations](aggregation.md) — `IAggregation`, the leaf-level counterpart for table-side accumulation
- [Operators Factory](operators-factory.md) — injecting Spring beans into `ICombination` implementations
- [Custom Measures](custom-measure.md) — when `ICombination` is not enough and a full `AMeasureQueryStep` is needed

