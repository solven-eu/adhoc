# Shiftor

A `Shiftor` is a measure that evaluates its underlying at a **different slice** — one produced by
mutating the current filter through an `IFilterEditor`. The result is written back to the original
(un-shifted) coordinates.

This is the standard building block for time-series comparisons (previous day, prior month-end,
same day last year) and for cross-sectional lookups (fetch the EUR value for whatever currency the
current row holds).

---

## Motivation: previous-day delta

Suppose the query groups by date `d` and the user wants the day-over-day change in `pnl`. A
`Combinator` can compute `pnl_today - pnl_yesterday`, but only if `pnl_yesterday` is a separate
measure that knows how to fetch the value for `d - 1`. That is exactly what `Shiftor` provides:

```java
// Step 1 — the shifted leaf measure
Shiftor pnlYesterday = Shiftor.builder()
		.name("pnl.yesterday")
		.underlying("pnl")
		.editorKey(PreviousDayEditor.class.getName())
		.build();

// Step 2 — the delta (Combinator over the two)
Combinator delta = Combinator.builder()
		.name("pnl.delta")
		.underlyings(List.of("pnl", "pnl.yesterday"))
		.combinationKey(SubtractionCombination.KEY)
		.build();
```

For each `{d=2025-04-05}` slice, `pnl.yesterday` fetches the `pnl` value at `{d=2025-04-04}`.

---

## The IFilterEditor contract

The shift logic lives in `IFilterEditor`:

```java
@FunctionalInterface
public interface IFilterEditor {
	ISliceFilter editFilter(ISliceFilter filter);

	default ISliceFilter editFilter(FilterEditorContext ctx) {
		return editFilter(ctx.getFilter());
	}
}
```

It receives the filter that describes the current slice and returns a new filter describing the
target slice. A typical implementation for a one-day shift:

```java
public class PreviousDayEditor implements IFilterEditor {

	@Override
	public ISliceFilter editFilter(ISliceFilter filter) {
		IValueMatcher dateMatcher = FilterHelpers.getValueMatcher(filter, "d");

		if (dateMatcher instanceof EqualsMatcher eq) {
			LocalDate yesterday = ((LocalDate) eq.getOperand()).minusDays(1);
			return SimpleFilterEditor.shift(filter, "d", yesterday);
		}
		return filter; // no date coordinate — return unchanged
	}
}
```

`SimpleFilterEditor.shift(filter, column, newValue)` replaces the equality constraint on `column`
with a new value, leaving the rest of the filter intact.

### Business-day logic

Shifting by one calendar day is rarely sufficient in finance. A real implementation wraps a
business-calendar lookup:

```java
LocalDate prevBd = calendar.previousBusinessDay(currentDate);
return SimpleFilterEditor.shift(filter, "d", prevBd);
```

The `IFilterEditor` is a plain Java class with no framework constraints, so injecting a
`BusinessCalendar` dependency is straightforward.

### Context-aware shifting with `customMarker`

Some shifts depend on context that is not encoded in the filter. `FilterEditorContext` carries an
optional `customMarker` for this purpose:

```java
@Override
public ISliceFilter editFilter(FilterEditorContext ctx) {
	String targetCcy = (String) ctx.getCustomMarker(); // e.g. "EUR"
	return SimpleFilterEditor.shift(ctx.getFilter(), "ccy", targetCcy);
}
```

The `customMarker` is set on the `CubeQueryStep` and is typically passed in by a wrapping
`Dispatchor` or by the query itself.

---

## How the shifted value is routed

`ShiftorQueryStep` requests **two** underlying cuboids for each step:

1. **Shifted cuboid** — the underlying measure evaluated with the shifted filter.
2. **Natural cuboid** — the underlying measure evaluated with the unmodified filter (used only to
   resolve *which* natural slices exist, so the result can be placed at the correct output
   coordinates).

For each natural slice `s`, the step:

1. Computes the shifted slice `s'` by applying the editor to `s`.
2. Reads the value from the shifted cuboid at `s'`.
3. Writes that value to the output at the original `s`.

The caller sees `pnl.yesterday` with the same groupBy as `pnl`; the internal double-query is
invisible.

---

## Builder syntax

```java
Shiftor.builder()
		.name("pnl.yesterday")
		.underlying("pnl")                           // single underlying (always one for Shiftor)
		.editorKey(PreviousDayEditor.class.getName()) // IFilterEditor implementation
		// .editorOptions(Map.of("calendar", "NYSE")) // optional config, passed to constructor
		.build()
```

For simple inline cases, use the `lambda` shortcut:

```java
Shiftor.builder()
		.name("pnl.yesterday")
		.underlying("pnl")
		.lambda(filter -> SimpleFilterEditor.shift(filter, "d",
				FilterHelpers.getDate(filter, "d").minusDays(1)))
		.build()
```

The `lambda` helper stores the editor as a `LambdaEditor` under the hood.

---

## Use case: Day2Day delta

A **Day2Day** measure reports `value_today - value_yesterday` for every date in the result. It
combines a `Shiftor` (to fetch the prior-day value) with a `Combinator` (to subtract):

```java
// 1. Leaf measure — raw daily value
Aggregator pnl = Aggregator.builder()
		.name("pnl")
		.aggregationKey(SumAggregation.KEY)
		.build();

// 2. Shifted measure — pnl evaluated one day earlier
Shiftor pnlYesterday = Shiftor.builder()
		.name("pnl.yesterday")
		.underlying("pnl")
		.lambda(filter -> SimpleFilterEditor.shift(filter, "d",
				FilterHelpers.getDate(filter, "d").minusDays(1)))
		.build();

// 3. Delta — today minus yesterday
Combinator pnlDay2Day = Combinator.builder()
		.name("pnl.day2day")
		.underlyings(List.of("pnl", "pnl.yesterday"))
		.combinationKey(SubtractionCombination.KEY)
		.build();
```

Querying `pnl.day2day GROUP BY d` produces:

|     d      | pnl | pnl.yesterday | pnl.day2day |
|------------|-----|---------------|-------------|
| 2025-04-03 | 100 | 80            | +20         |
| 2025-04-04 | 80  | 110           | −30         |
| 2025-04-05 | 130 | 80            | +50         |

`pnl.yesterday` is `null` for the earliest date in the result (no prior row exists); the
subtraction therefore also returns `null` for that row, which is the expected behaviour.

For a business-day-aware variant, replace the `lambda` with a named `IFilterEditor` class that
consults a holiday calendar (see the business-day section above). The `Combinator` layer is
unchanged — only the shift logic needs to change.

---

## Comparison with Filtrator

|                        |             Filtrator             |                    Shiftor                     |
|------------------------|-----------------------------------|------------------------------------------------|
| **What changes**       | AND-adds a hardcoded filter       | Replaces/mutates the existing filter           |
| **Typical use**        | "Always restrict to EUR"          | "Fetch the value from the previous day"        |
| **Output coordinates** | Same as input                     | Same as input (value read from shifted coords) |
| **Filter produced**    | `query.filter AND measure.filter` | `editor(query.filter)`                         |

A `Filtrator` narrows the filter; a `Shiftor` *redirects* it.

---

## Further reading

- `TestCubeQuery_Shiftor` — standard shift behaviour: groupBy, grand total, filter interaction
- `TestTransformator_Shiftor_contextValue` — context-aware shifting with `customMarker`
- `TestTransformator_Shiftor_Perf` — `PreviousDayEditor` over 10 000 dates with DuckDB
- `SimpleFilterEditor` — utility methods `shift()` and `shiftIfPresent()` for common patterns
- [Filtrator](filtrator.md) — the common specialisation that ANDs a fixed filter without a custom `IFilterEditor`
- [Unfiltrator](unfiltrator.md) — the counterpart that widens filters by removing constraints
- [Hierarchies](hierarchies.md) — using Shiftor to navigate parent levels in a hierarchy
- [Concepts → Measure archetypes](concepts.md) — overview of all measure types
- [Slice and IAdhocMap](slice.md) — how `ISliceFilter` is structured and what `shift()` modifies

