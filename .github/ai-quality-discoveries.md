# AI Quality Discoveries — 2026-03-09

This file records every code-quality pattern encountered during this run.
Each entry includes a representative location, occurrence count, and resolution status.

---

## 1. `List.of(…)` → `ImmutableList.of(…)`

**Description**: JDK `List.of()` used instead of Guava `ImmutableList.of()` for literal or single-return-value list construction.

**Example**: `adhoc/src/main/java/eu/solven/adhoc/measure/decomposition/many2many/ManyToMany1DDecomposition.java:225`

```java
// Before
return List.of(MeasurelessQuery.edit(step)…build());
// After
return ImmutableList.of(MeasurelessQuery.edit(step)…build());
```

**Occurrences**: ~30 across 15+ files

**Status**: applied

**Files changed**:
- `libraries/measures/src/main/java/eu/solven/adhoc/measure/decomposition/LinearDecomposition.java`
- `libraries/measures/src/main/java/eu/solven/adhoc/util/AdhocIdentity.java`
- `libraries/measures/src/main/java/eu/solven/adhoc/measure/model/Filtrator.java`
- `libraries/measures/src/main/java/eu/solven/adhoc/measure/model/Dispatchor.java`
- `libraries/measures/src/main/java/eu/solven/adhoc/measure/model/Shiftor.java`
- `libraries/measures/src/main/java/eu/solven/adhoc/measure/model/Unfiltrator.java`
- `adhoc/src/main/java/eu/solven/adhoc/measure/decomposition/many2many/ManyToMany1DDecomposition.java`
- `adhoc/src/main/java/eu/solven/adhoc/measure/decomposition/many2many/ManyToManyNDDecomposition.java`
- `adhoc/src/main/java/eu/solven/adhoc/measure/transformator/step/FiltratorQueryStep.java`
- `adhoc/src/main/java/eu/solven/adhoc/measure/transformator/step/UnfiltratorQueryStep.java`
- `adhoc/src/main/java/eu/solven/adhoc/example/worldcup/WorldCupPlayersSchema.java`
- `adhoc/src/main/java/eu/solven/adhoc/engine/QueryStepsDagBuilder.java`

---

## 2. `Map.of(…)` → `ImmutableMap.of(…)`

**Description**: JDK `Map.of()` used instead of Guava `ImmutableMap.of()`. Not explicitly listed in CONVENTIONS.MD but consistent with the stated principle "prefer Guava immutable collections over JDK factories".

**Example**: `libraries/measures/src/main/java/eu/solven/adhoc/util/AdhocIdentity.java:76`

```java
// Before
return Map.of();
// After
return ImmutableMap.of();
```

**Occurrences**: ~25 across 12 files

**Status**: applied

**Files changed**:
- `libraries/measures/src/main/java/eu/solven/adhoc/util/AdhocIdentity.java`
- `libraries/measures/src/main/java/eu/solven/adhoc/measure/model/Shiftor.java`
- `libraries/measures/src/main/java/eu/solven/adhoc/measure/aggregation/comparable/RankAggregation.java`
- `libraries/measures/src/main/java/eu/solven/adhoc/measure/decomposition/LinearDecomposition.java`
- `adhoc/src/main/java/eu/solven/adhoc/measure/decomposition/many2many/ManyToMany1DDecomposition.java`
- `adhoc/src/main/java/eu/solven/adhoc/measure/decomposition/many2many/ManyToManyNDDecomposition.java`
- `adhoc/src/main/java/eu/solven/adhoc/example/worldcup/WorldCupPlayersSchema.java`
- `adhoc/src/main/java/eu/solven/adhoc/example/worldcup/DispatchedEvents.java`
- `libraries/filters/src/main/java/eu/solven/adhoc/query/filter/FilterHelpers.java`

---

## 3. `Set.of(array)` → `ImmutableSet.copyOf(array)`

**Description**: `Set.of(varargs)` used where `ImmutableSet.copyOf(array)` is both correct and consistent with the Guava-first convention. The existing usage created a new Set on each stream-predicate invocation.

**Example**: `atoti/src/main/java/eu/solven/adhoc/atoti/translation/AtotiMeasureToAdhoc.java:619`

```java
// Before
.filter(k -> !Set.of(excludedProperties).contains(k))
// After
.filter(k -> !ImmutableSet.copyOf(excludedProperties).contains(k))
```

**Occurrences**: 2 (both in `AtotiMeasureToAdhoc.java`)

**Status**: applied

---

## 4. `new ArrayList<>(N)` for fixed-size immutable lists → `ImmutableList.of(…)` / stream+`toImmutableList()`

**Description**: `new ArrayList<>()` populated with a fixed set of entries then returned. Can be replaced with `ImmutableList.of(…)` for clarity and safety.

**Example**: `libraries/measures/src/main/java/eu/solven/adhoc/measure/decomposition/LinearDecomposition.java:92–97`

```java
// Before
List<IDecompositionEntry> output = new ArrayList<>(2);
output.add(IDecompositionEntry.of(Map.of(outputColumn, min), scale(…)));
output.add(IDecompositionEntry.of(Map.of(outputColumn, max), scaleComplement(…)));
return output;
// After
return ImmutableList.of(
	IDecompositionEntry.of(ImmutableMap.of(outputColumn, min), scale(…)),
	IDecompositionEntry.of(ImmutableMap.of(outputColumn, max), scaleComplement(…)));
```

**Occurrences**: ~3 clear cases applied; ~50 `new ArrayList<>` calls across the codebase remain where mutation is genuinely needed

**Status**: applied (where clearly constructing a fixed-size list for immediate return)

---

## 5. `forEach` + `List.add` → `stream().map(…).collect(ImmutableList.toImmutableList())`

**Description**: Mutable `ArrayList` built via `forEach` then returned. The stream-based equivalent is more idiomatic and produces an immutable result.

**Example**: `adhoc/src/main/java/eu/solven/adhoc/measure/decomposition/many2many/ManyToManyNDDecomposition.java:153–159`

```java
// Before
List<IDecompositionEntry> output = new ArrayList<>(groups.size());
groups.forEach(group -> {
	output.add(IDecompositionEntry.of(Map.of(groupColumn, group), scale(element, value)));
});
return output;
// After
return groups.stream()
	.map(group -> IDecompositionEntry.of(ImmutableMap.of(groupColumn, group), scale(element, value)))
	.collect(ImmutableList.toImmutableList());
```

**Occurrences**: 2 (ManyToManyNDDecomposition, ManyToMany1DDecomposition)

**Status**: applied

---

## 6. `Map.of(…)` → `ImmutableMap.of(…)` — not listed in CONVENTIONS.MD

**Description**: CONVENTIONS.MD lists `Set.of` and `List.of` as explicit replacements, but does not mention `Map.of`. The "Guava over JDK" principle applies equally to `Map`. Recommend adding `ImmutableMap.of(…) instead of Map.of(…)` to CONVENTIONS.MD.

**Example**: Multiple files throughout the codebase.

**Occurrences**: ~25

**Status**: applied — consider promoting to CONVENTIONS.MD

---

## 7. `new ArrayList<>` accumulator for nested forEach — skipped

**Description**: `adhoc/src/main/java/eu/solven/adhoc/example/worldcup/DispatchedEvents.java:79` builds a list via nested `forEach` (two-level iteration over a `Map<String, Map<Integer, Long>>`). Converting to streams would require a `flatMap` which increases complexity without clear benefit for readability.

**Example**: `DispatchedEvents.java:79`

```java
List<IDecompositionEntry> decompositions = new ArrayList<>();
playerEvents.getTypeToMinuteToCount().forEach((eventCode, minuteToCount) -> {
	minuteToCount.forEach((minute, count) -> {
		decompositions.add(…);
	});
});
```

**Occurrences**: 1

**Status**: skipped — needs human review; nested forEach pattern with captured mutable list; stream refactor adds indirection for limited gain

---

## 8. `Collectors.toList()` / `Collectors.toSet()` — none found in main sources

**Description**: The grep for `.collect(Collectors.toList())` and `.collect(Collectors.toSet())` in main sources returned only 5 hits in pivotable/server-core and calcite modules; not in the core adhoc module. Each appears inside registry-style classes where the list is immediately consumed or passed to a mutable builder.

**Occurrences**: 5 (not applied this run; target for a follow-up run)

**Status**: skipped — needs human review; the collections may be consumed immediately by mutable contexts; lower confidence than pure `List.of` replacements

---

## 9. Remaining `\bList.of\(` / `\bSet.of\(` / `\bMap.of\(` after this run

After applying changes, the following files still have JDK factory usage that was not edited (due to the 20-file-per-run cap or low confidence):

- `adhoc/src/main/java/eu/solven/adhoc/table/sql/duckdb/DuckDBHelper.java` (2× `Map.of`)
- `adhoc/src/main/java/eu/solven/adhoc/table/composite/CompositeCubesTableWrapper.java` (2× `Map.of`)
- `libraries/cell/src/main/java/eu/solven/adhoc/map/MaskedAdhocMap.java` (2× `Map.of`)
- `libraries/dataframe/src/main/java/eu/solven/adhoc/data/tabular/MapBasedTabularView.java` (3× `Map.of`)
- `pivotable/server/src/main/java/eu/solven/adhoc/pivotable/webflux/api/PivotableLoginController.java` (6× `Map.of`)
- `libraries/dataframe/src/main/java/eu/solven/adhoc/data/row/TabularRecordOverMaps.java` (1× `Map.of`)
- `libraries/dataframe/src/main/java/eu/solven/adhoc/table/transcoder/value/StandardCustomTypeManager.java` (1× `Map.of`)

**Status**: skipped — cap reached; target for next run
