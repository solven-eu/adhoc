# Custom Measures

Every built-in measure type (`Combinator`, `Filtrator`, `Shiftor`, …) is itself an implementation
of the same extension point. The same mechanism is available to application code: any measure with
arbitrary evaluation logic can be plugged in without forking the engine.

---

## The two moving parts

A custom measure requires two classes:

|         Class         |                                                Role                                                |
|-----------------------|----------------------------------------------------------------------------------------------------|
| **Measure** (data)    | Holds configuration: name, underlying measure names, options. Implements `IHasUnderlyingMeasures`. |
| **QueryStep** (logic) | Holds evaluation logic. Extends `AMeasureQueryStep`. Named `<Measure>QueryStep` by convention.     |

The engine wires them together: when it encounters a measure whose class implements
`IHasUnderlyingMeasures`, it looks up the corresponding `QueryStep` class via
`measure.queryStepClass()` and instantiates it reflectively.

---

## IHasUnderlyingMeasures

```java
public interface IHasUnderlyingMeasures extends IHasUnderlyingNames {

	/** Declares the underlying measure names this measure depends on. */
	List<String> getUnderlyingNames();

	/**
	 * Returns the fully-qualified name of the AMeasureQueryStep class that
	 * evaluates this measure. The default convention maps
	 * {@code com.example.Foo} → {@code ...step.FooQueryStep}.
	 */
	default String queryStepClass() {
		return "eu.solven.adhoc.measure.transformator.step.%sQueryStep"
				.formatted(this.getClass().getSimpleName());
	}
}
```

Override `queryStepClass()` to break the naming convention or to place the `QueryStep` in a
different package.

---

## AMeasureQueryStep — the evaluation contract

```java
public abstract class AMeasureQueryStep implements IMeasureQueryStep {

	/** The query context: measure, groupBy, filter, options. */
	public abstract CubeQueryStep getStep();

	/** Factory access: column construction, slice factory, operator factory. */
	public abstract IAdhocFactories getFactories();

	/**
	 * Returns the CubeQueryStep for each underlying measure the engine must
	 * evaluate before this step can run. May request the same underlying
	 * multiple times (e.g. Shiftor requests it twice — once shifted, once not).
	 */
	public abstract List<CubeQueryStep> getUnderlyingSteps();

	/**
	 * Produces the output ICuboid given the already-evaluated underlying
	 * cuboids, in the same order as getUnderlyingSteps().
	 */
	public abstract ICuboid produceOutputColumn(List<? extends ICuboid> underlyings);
}
```

### ICuboid — the result container

`ICuboid` is a read-only map from `ISlice` (a set of column coordinates) to a value:

```java
IValueProvider value = cuboid.onValue(slice);   // read value at a slice
cuboid.stream()                                  // iterate all (slice, value) pairs
	.forEach(sam -> ...);
boolean missing = cuboid.isEmpty();
```

`produceOutputColumn` must return a new `ICuboid` built from scratch using the underlying
cuboids.

---

## Example: RouterMeasure

Consider a portfolio system where data quality improved significantly after a cut-over date. Before
`2025-12-31`, position data came from a legacy source (`pnl.legacy`); from `2026-01-01` onward it
comes from a new source (`pnl.new`). A user querying across both periods wants the engine to
select the right measure automatically based on the `asOf` coordinate.

### The measure (data class)

```java
@Value
@Builder(toBuilder = true)
@Jacksonized
public class RouterMeasure implements IMeasure, IHasUnderlyingMeasures {

	@NonNull String name;

	@NonNull @Singular @With ImmutableSet<String> tags;

	/** The measure to use for asOf <= cutoverDate. */
	@NonNull String beforeMeasure;

	/** The measure to use for asOf > cutoverDate. */
	@NonNull String afterMeasure;

	/** The column carrying the as-of date (typically "asOf"). */
	@NonNull @Default String asOfColumn = "asOf";

	/** The cut-over date. Slices with asOf on or before this date use beforeMeasure. */
	@NonNull LocalDate cutoverDate;

	@JsonIgnore
	@Override
	public List<String> getUnderlyingNames() {
		return List.of(beforeMeasure, afterMeasure);
	}
}
```

The `queryStepClass()` default resolves to `RouterMeasureQueryStep`, which the engine will
instantiate by convention.

### The query step (logic class)

```java
@RequiredArgsConstructor
public class RouterMeasureQueryStep extends AMeasureQueryStep {

	final RouterMeasure measure;
	@Getter final CubeQueryStep step;
	@Getter(AccessLevel.PROTECTED) final IAdhocFactories factories;

	@Override
	public List<CubeQueryStep> getUnderlyingSteps() {
		// Request both underlyings with identical filter/groupBy.
		// The engine evaluates them; produceOutputColumn picks the right one per slice.
		return List.of(
				CubeQueryStep.edit(step).measure(measure.getBeforeMeasure()).build(),
				CubeQueryStep.edit(step).measure(measure.getAfterMeasure()).build());
	}

	@Override
	public ICuboid produceOutputColumn(List<? extends ICuboid> underlyings) {
		ICuboid beforeCuboid = underlyings.get(0);
		ICuboid afterCuboid  = underlyings.get(1);

		IMultitypeColumnFastGet<ISlice> output =
				factories.getColumnFactory().makeColumn(beforeCuboid.size());

		// Iterate the union of all slices present in either cuboid
		forEachDistinctSlice(underlyings, (sliceAndMeasures) -> {
			ISlice slice = sliceAndMeasures.getSlice().getSlice();

			// Determine which cuboid to read based on the asOf coordinate
			ICuboid source = route(slice) ? beforeCuboid : afterCuboid;
			source.onValue(slice).acceptReceiver(output.putSlice(slice));
		});

		return Cuboid.forGroupBy(step).values(output).build();
	}

	/** Returns true if the slice belongs to the "before" period. */
	private boolean route(ISlice slice) {
		Object raw = slice.getRaw(measure.getAsOfColumn());
		if (raw instanceof LocalDate asOf) {
			return !asOf.isAfter(measure.getCutoverDate());
		}
		// No asOf coordinate in this slice — default to the after-measure
		return false;
	}
}
```

### Assembling the forest

```java
RouterMeasure router = RouterMeasure.builder()
		.name("pnl.routed")
		.beforeMeasure("pnl.legacy")
		.afterMeasure("pnl.new")
		.asOfColumn("asOf")
		.cutoverDate(LocalDate.of(2025, 12, 31))
		.build();

forest.addMeasure(router);
```

Querying `pnl.routed GROUP BY desk, asOf` now transparently uses `pnl.legacy` for rows where
`asOf ≤ 2025-12-31` and `pnl.new` for rows where `asOf > 2025-12-31`.

---

## Key patterns from built-in measure types

The same infrastructure drives all built-in types. Their `getUnderlyingSteps()` patterns are
worth knowing when implementing custom measures:

| Measure type  |                                getUnderlyingSteps() pattern                                |
|---------------|--------------------------------------------------------------------------------------------|
| `Combinator`  | One step per underlying, same filter and groupBy                                           |
| `Filtrator`   | One step with `query.filter AND measure.filter`                                            |
| `Shiftor`     | Two steps: one with shifted filter (to read), one unshifted (to resolve write coordinates) |
| `Partitionor` | One step per underlying with a *widened* groupBy (union of query + measure groupBy)        |

---

## When to implement a custom measure

Use a custom measure when none of the built-in types can express the logic:

- **Conditional routing** — select an underlying based on a coordinate value or date range (as above)
- **Cross-slice aggregation** — read values from multiple slices and combine them in a way that `Partitionor` cannot express
- **External lookup with complex state** — an operator that needs rich collaborator objects not easily injected via `IOperatorsFactory`
- **Dynamic underlying selection** — the set of underlyings is only known at query time, based on the current filter

For cases where a pluggable function is sufficient, prefer `Combinator` (with a custom
`ICombination`), `Shiftor` (with a custom `IFilterEditor`), or `Dispatchor` (with a custom
`IDecomposition`) — they are simpler and require no `QueryStep` class.

---

## Further reading

- [Operators Factory](operators-factory.md) — injecting collaborators (including Spring beans) into combinations and filter editors
- [Shiftor](shiftor.md) — the routing pattern applied to filter transformation
- [Partitionor](partitionor.md) — the groupBy-widening pattern
- [Concepts → Measure archetypes](concepts.md) — overview of all built-in measure types

