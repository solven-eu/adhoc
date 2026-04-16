# Custom Markers

A **custom marker** is an arbitrary `Object` carried by a query that lets the user customise the
behaviour of measures *per-query*, without changing the measure definition. Typical use cases are a
target currency, a calendar, a "what-if" scenario tag, or any small piece of context the
[Combination](combination.md) / [Shiftor](shiftor.md) / [Partitionor](partitionor.md) needs to read
at evaluation time.

## What it is

- It is set on the [`CubeQuery`](cube-query-engine.md), via `CubeQueryBuilder.customMarker(Object)`.
- It is propagated by the engine to **every `CubeQueryStep`** of the DAG. Every step in the tree
  carries the same marker — measures further down the tree see the value chosen by the user.
- It is **typed `Object`**, so any POJO works. An `Optional` is unwrapped on the way in to avoid
  `Optional<Optional<?>>` noise.
- It is part of the `CubeQueryStep` identity: two steps with different markers are different steps,
  so the engine's per-step deduplication and caching stay correct.
- Inside a measure (typically a `Combinator`), read it via
  `slice.getQueryStep().getCustomMarker()` (raw) or `slice.getQueryStep().optCustomMarker()`
  (wrapped in an `Optional`).

```java
CubeQuery query = CubeQuery.builder()
		.measure("amount.in_target_ccy")
		.customMarker(Map.of("targetCcy", "USD"))
		.build();
```

## Adhoc is *not* in the business of typing the marker

Adhoc never inspects the marker's content — it just propagates it. Concretely, the engine's only
contract is "an `Object` (not an `Optional`) that is part of the step's identity". Two consequences:

1. **Anything is a valid marker** — a `String`, a `Map`, a domain POJO, a `record`, ... It is up to
   the measure that reads it to interpret the value.
2. **Identity matters** — `equals`/`hashCode` of the marker drive step deduplication and caching.
   Mutable markers (e.g. a `HashMap` you keep mutating) defeat both. Prefer immutable maps or
   records.

## Reading a marker — the common patterns

### Plain `getCustomMarker()`

```java
public class MyCombination implements ICombination {
	@Override
	public Object combine(ISliceWithStep slice, List<?> underlyings) {
		Object marker = slice.getQueryStep().getCustomMarker();
		if (marker instanceof Map<?, ?> map) {
			String targetCcy = (String) map.get("targetCcy");
			// ... use targetCcy
		}
		return underlyings.getFirst();
	}
}
```

### `ACustomMarkerCombination` (JSONPath-style lookup)

For combinations that read a single value at a known path inside a `Map<String, ?>` marker, extend
[`ACustomMarkerCombination`](../adhoc/src/main/java/eu/solven/adhoc/measure/custom_marker/ACustomMarkerCombination.java)
and just declare the path:

```java
class TargetCcyCombination extends ACustomMarkerCombination {
	@Override
	protected String getJoinedMapPath() {
		return "$.targetCcy";
	}

	@Override
	protected Object getDefault() {
		return "EUR";
	}
}
```

The base class handles the null-marker case, the recursive `Map`-of-`Map` case, and the
flat-key case. It is the recommended starting point.

### `FilterEditor` context

`Shiftor` / `IFilterEditor` get the marker through `FilterEditorContext.getCustomMarker()` — same
value, different access path. See [Shiftor § Context-aware shifting with `customMarker`](shiftor.md#context-aware-shifting-with-custommarker).

## Classic example: foreign-exchange reference currency

The textbook use case for a `customMarker` is a **user-configurable reference currency**.
Consider facts that arrive in many native currencies (`USD`, `EUR`, `JPY`, ...) and need to be
reported in a single output currency chosen by the user at query time. The reference currency
**must not be hardcoded in the measure definitions** — two users of the same report want to see
their own currency.

A typical measure tree:

- **Leaf**: `SUM(amount)` aggregated by the native `ccyFrom` column — each native currency
  aggregates independently, no FX yet.
- **FX node**: a `Partitionor` whose combination reads
  `slice.getQueryStep().getCustomMarker()` to determine the target currency (`ccyTo`), looks up
  the `fromCcy × toCcy` rate, multiplies, and sums the converted contributions. If conversion to
  the user's target currency is not directly available, the combination can first convert to a
  pivot currency (`USD`, `EUR`) and then chain another FX hop to the final reference.
- **Root**: the amount in the user's chosen currency.

The user controls `ccyTo` on the way in — the value propagates down to every `CubeQueryStep` of
the DAG, so the FX combination reads the same marker regardless of its position in the tree:

```java
CubeQuery query = CubeQuery.builder()
		.measure("amount.CCY")
		.customMarker("JPY") // ← the reference currency for this query
		.build();
```

```java
String ccyTo = (String) slice.getQueryStep().getCustomMarker();
```

The full referential implementation lives in
[`ForeignExchangeCombination`](../adhoc/src/test/java/eu/solven/adhoc/query/foreignexchange/ForeignExchangeCombination.java)
(it accepts either a raw `String` like `"USD"` or a `Map` with a `ccyTo` key, and falls back to a
default when neither is present) and is exercised end-to-end in
[`TestCubeQueryFx`](../adhoc/src/test/java/eu/solven/adhoc/query/foreignexchange/TestCubeQueryFx.java).

### Forcing a currency for a specific measure

A common follow-up: alongside the user-chosen currency, always report one fixed reference currency
(e.g. *"corporate EUR"*) so a dashboard can show "your currency" and "EUR" side by side. The
trick is a wrapper measure that **rewrites the `customMarker` for its own subtree** before the
DAG is expanded further down:

```java
// Dynamic measure: reads whatever the user sent as `customMarker`.
forest.addMeasure(Partitionor.builder()
		.name("k1.CCY")
		.underlyings(List.of("k1.SUM"))
		.groupBy(GroupByColumns.named("ccyFrom"))
		.combinationKey(ForeignExchangeCombination.KEY)
		.build());

// Forced measure: always reports in EUR, regardless of what the user selected.
forest.addMeasure(CustomMarkerEditor.builder()
		.name("k1.EUR")
		.underlying("k1.CCY")
		.customMarkerEditor(opt -> Optional.of("EUR"))
		.build());
```

- User sets `customMarker = "JPY"` on the query.
- `k1.CCY` sees `"JPY"` → converts to JPY.
- `k1.EUR` rewrites the marker it hands to its underlying to `"EUR"`; the **same** `k1.CCY`
  subtree re-evaluates under that edited step and converts to EUR. Both measures coexist in the
  same query without interfering because they produce different `CubeQueryStep` identities (the
  marker is part of the step's `equals`/`hashCode` — see [§ What it is](#what-it-is)).

The referential implementation is
[`CustomMarkerEditor`](../adhoc/src/test/java/eu/solven/adhoc/query/custommarker/CustomMarkerEditor.java)
(a test-scope `IHasUnderlyingMeasures` measure type demonstrating the pattern), and the end-to-end
behaviour — including the three-column "dynamic / forced-EUR / forced-USD" matrix — is covered by
[`TestCustomMarkerEnforcer`](../adhoc/src/test/java/eu/solven/adhoc/query/custommarker/TestCustomMarkerEnforcer.java).

The pattern generalises beyond FX: any time a measure needs to run its subtree *as if* the user
had configured a different marker (scenario override, "what-if" toggle, frozen snapshot date, ...),
an editor measure in front of the subtree is the simplest tool.

## Transcoding raw markers — `ICustomMarkerTranscoder`

When a query is received over the wire (e.g. as JSON), the marker arrives as a raw `Map<String,
Object>` — Jackson has no way to know which POJO to deserialise it into. If the measure expects a
typed object, the schema can install an
[`ICustomMarkerTranscoder`](../adhoc/src/main/java/eu/solven/adhoc/beta/schema/ICustomMarkerTranscoder.java)
to convert the raw map into the typed form before the engine hands it to the measures:

```java
AdhocSchema schema = AdhocSchema.builder()
		.env(env)
		.engine(engine)
		.customMarkerCleaner((cubeWrapper, raw) -> {
			if (raw instanceof Map<?, ?> map) {
				return new MyMarker((String) map.get("targetCcy"), (Boolean) map.get("debug"));
			}
			return raw;
		})
		.build();
```

`AdhocSchema.execute(...)` runs the transcoder once at query entry; the resulting typed marker is
then placed on the `CubeQueryStep` and seen by every measure in the DAG. The transcoder receives
the target `ICubeWrapper` so a single schema can apply different transcoding rules per cube.

A unit-test of this end-to-end flow lives in
[`TestAdhocSchema.testCustomMarker_rawMapToTypedRecord`](../adhoc/src/test/java/eu/solven/adhoc/beta/schema/TestAdhocSchema.java).

## Schema-side metadata for UIs — `CustomMarkerMetadata`

So that a UI (such as Pivotable) can offer the right input for a given cube, the schema can declare
which markers a cube understands via
`AdhocSchema.registerCustomMarker(String name, IValueMatcher cubeMatcher, CustomMarkerMetadataGenerator)`.
The generator declares the JSONPath to the data point, the set of allowed values, and a default —
the schema metadata returned by `getMetadata(...)` then exposes a `Map<String, CustomMarkerMetadata>`
per cube, which the UI can render as a dropdown / text input. See `CustomMarkerMetadataGenerator`
and `AdhocSchema.CustomMarkerMatchingKey`.

## Customising `AdhocSchema` from a Spring application — `IAdhocSchemaCustomizer`

`AdhocSchema` is built by Pivotable's `InjectPivotableSelfEndpointConfig.registerSelfSchema(...)`
([source](../pivotable/server-core/src/main/java/eu/solven/adhoc/pivotable/app/InjectPivotableSelfEndpointConfig.java)).
That bean walks the application context and applies every registered
[`IAdhocSchemaCustomizer`](../pivotable/server-core/src/main/java/eu/solven/adhoc/pivotable/app/IAdhocSchemaCustomizer.java)
to the schema's builder before `build()` is called, so each customizer can install its own
`customMarkerCleaner`, register `CustomMarkerMetadata` entries, or add tables / forests / cubes.

```java
@Bean
public IAdhocSchemaCustomizer<AdhocSchemaBuilder> myProjectCustomizer() {
	return builder -> builder
			.customMarkerCleaner(MyProjectCustomizer::transcodeMarker)
			// also: tagColumn / registerCustomMarker / ...
			;
}

private static Object transcodeMarker(ICubeWrapper cube, Object raw) {
	if (raw instanceof Map<?, ?> map) {
		return new MyMarker((String) map.get("targetCcy"));
	}
	return raw;
}
```

Multiple customizers can coexist: they are applied in arbitrary bean-resolution order, so each
customizer must be additive (`tagColumn`, `registerCustomMarker`, ...) rather than override the
previous one.

## Caching and `ICustomMarkerCacheStrategy`

Because the marker is part of the step's identity, two queries that differ only by their marker
hit two different cache entries. If a measure is *insensitive* to a part of the marker, you can
override `ICustomMarkerCacheStrategy` on the table wrapper to project the marker down to a
cache-relevant subset before caching — see `CachingTableWrapper`.

## Summary

|                        Question                        |                                        Answer                                        |
|--------------------------------------------------------|--------------------------------------------------------------------------------------|
| Where does the user set it?                            | `CubeQueryBuilder.customMarker(Object)`                                              |
| Where is it stored?                                    | On every `CubeQueryStep` in the DAG                                                  |
| How does a measure read it?                            | `slice.getQueryStep().getCustomMarker()` / `optCustomMarker()`                       |
| What type can it be?                                   | Any `Object` except `Optional` (it is unwrapped). Typically a `Map` or a record.     |
| How to convert a raw `Map` (from JSON) into a POJO?    | `AdhocSchema.builder().customMarkerCleaner(ICustomMarkerTranscoder)`                 |
| How to advertise the supported markers to a UI?        | `AdhocSchema.registerCustomMarker(name, cubeMatcher, CustomMarkerMetadataGenerator)` |
| How to install all of the above from a Spring context? | One or more `IAdhocSchemaCustomizer` beans, picked up by `registerSelfSchema(...)`   |

## See also

- [Combination](combination.md) — the most common reader of customMarkers.
- [Shiftor](shiftor.md) — context-aware filter edition driven by a customMarker.
- [Partitionor](partitionor.md) — splits the slice and dispatches to a per-partition combination,
  often parameterised by a marker.
- [CubeQueryEngine](cube-query-engine.md) — where the `customMarker` field lives on `CubeQueryStep`.

