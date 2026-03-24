# AI Quality Discoveries

Run date: 2026-03-23
Branch: `ai-quality-agent/nightly-23421980413`
Scope: all `src/main/java` files (non-test), Pass 1 (grep-detectable patterns) + Pass 2 (Stepdown Rule on 10 most-recently-modified files).

---

## Pass 1 — Grep-Detectable Anti-Patterns

### 1. JDK `List.of(...)` instead of `ImmutableList.of(...)`

|         Field         |                                 Value                                 |
|-----------------------|-----------------------------------------------------------------------|
| **Pattern**           | `[^a-zA-Z]List\.of\(`                                                 |
| **Convention**        | CONVENTIONS.MD §Guava immutable collections                           |
| **Example**           | `StandardDSLSupplier.java:53` — `.addAll(List.of(existingProviders))` |
| **Occurrences found** | ~10 across main sources (JMH benchmarks excluded from fix scope)      |
| **Status**            | applied                                                               |

**Files fixed:**
- `adhoc/…/StandardDSLSupplier.java` — `.addAll(ImmutableList.copyOf(existingProviders))`
- `pivotable/…/PivotableChatHandler.java` — 3 occurrences in `buildTools()`
- `adhoc/…/TableQueryEngineBootstrapped.java` — `ImmutableList.of(tableQuery)` in `splitForNonAmbiguousColumns`
- `pivotable/…/InjectPixarExampleCubesConfig.java` — `.leftJoin(…, ImmutableList.of(Map.entry(…)))`
- `libraries/dataframe/…/MultitypeArray.java` — `ImmutableList.of()` in `empty()`
- `libraries/dataframe/…/UnderlyingQueryStepHelpersNavigableElseHash.java` — `ImmutableList.of(value)` in fast-track path
- `experimental/…/UnderlyingQueryStepHelpersNavigable.java` — same pattern as above

**Skipped (needs human review):**
- `jmh/…/BenchmarkInFilter.java`, `BenchmarkColumnarSliceFactory.java`, `BenchmarkPerfectHashing.java`, `BenchmarkAdhocMapFactory.java` — JMH benchmark classes; JDK `List.of` is fine for benchmark setup data that is not part of production logic

---

### 2. JDK `Set.of(...)` instead of `ImmutableSet.of(...)`

|         Field         |                                     Value                                      |
|-----------------------|--------------------------------------------------------------------------------|
| **Pattern**           | `[^a-zA-Z]Set\.of\(`                                                           |
| **Convention**        | CONVENTIONS.MD §Guava immutable collections                                    |
| **Example**           | `LoggingAtotiWrapper.java:67` — `return Set.of(GetAggregatesQuery.PLUGIN_KEY)` |
| **Occurrences found** | ~6 across main sources                                                         |
| **Status**            | partially applied — 1 applied, 4 skipped (intentional), 2 in JMH               |

**Files fixed:**
- `atoti/…/LoggingAtotiWrapper.java` — `ImmutableSet.of(GetAggregatesQuery.PLUGIN_KEY)`

**Skipped (intentional — do NOT change):**
- `libraries/cell/…/ASliceFactory.java` — 4 occurrences register `Set.of` class references in `NOT_SEQUENCED_CLASSES` sentinel set to detect non-deterministically-ordered sets at runtime. Replacing these with `ImmutableSet.of` would break the sentinel logic.

**Skipped (needs human review):**
- `jmh/…/BenchmarkAdhocMapVsHashMap.java`, `BenchmarkAdhocMapComparateTo.java` — JMH benchmarks

---

### 3. `new HashMap<>()` instead of `new LinkedHashMap<>()`

|         Field         |                                            Value                                             |
|-----------------------|----------------------------------------------------------------------------------------------|
| **Pattern**           | `new HashMap[^(]*\(\)`                                                                       |
| **Convention**        | CONVENTIONS.MD §Ordered mutability                                                           |
| **Example**           | `CaseInsensitiveContext.java:42` — `final Map<String, String> lowerToCase = new HashMap<>()` |
| **Occurrences found** | 2 (1 in main sources, 1 in JMH benchmark)                                                    |
| **Status**            | partially applied                                                                            |

**Files fixed:**
- `adhoc/…/CaseInsensitiveContext.java` — `new LinkedHashMap<>()`

**Skipped (needs human review):**
- `jmh/…/BenchmarkAdhocMapVsHashMap.java` — JMH benchmark comparing map implementations; `HashMap` is intentional here as part of the benchmark subject

---

### 4. `new HashSet<>()` instead of `new LinkedHashSet<>()`

|         Field         |                           Value                           |
|-----------------------|-----------------------------------------------------------|
| **Pattern**           | `new HashSet[^(]*\(\)`                                    |
| **Convention**        | CONVENTIONS.MD §Ordered mutability                        |
| **Occurrences found** | 0 in main sources                                         |
| **Status**            | known (already in CONVENTIONS.MD) — no occurrences to fix |

---

### 5. `Collectors.toMap(kFn, vFn)` (2-arg) instead of `PepperStreamHelper.toLinkedMap`

|         Field         |                                                             Value                                                             |
|-----------------------|-------------------------------------------------------------------------------------------------------------------------------|
| **Pattern**           | `Collectors\.toMap\(`                                                                                                         |
| **Convention**        | CONVENTIONS.MD §PepperStreamHelper.toLinkedMap                                                                                |
| **Example**           | `FunctionCalculatedColumn.java:~76` — `columns.stream().collect(Collectors.toMap(Function.identity(), c -> RECORDING_VALUE))` |
| **Occurrences found** | 5 (3 applied, 2 already compliant — using 4-arg form)                                                                         |
| **Status**            | partially applied                                                                                                             |

**Files fixed:**
- `adhoc/…/FunctionCalculatedColumn.java` — `PepperStreamHelper.toLinkedMap(Function.identity(), c -> RECORDING_VALUE)`
- `adhoc/…/TabularRecordStreamReducer.java` — `PepperStreamHelper.toLinkedMap(IGroupBy::getGroupedByColumns, gb -> {...})`
- `adhoc/…/CompositeCubesTableWrapper.java` — `PepperStreamHelper.toLinkedMap(Entry::getKey, cubeAndQuery -> {...})`

**Already compliant (no change):**
- `IHasColumns.java` — uses 4-arg `Collectors.toMap` with merge function
- `TableQueryV3.java` — uses 4-arg `Collectors.toMap` with merge function

---

### 6. `Collectors.groupingBy(...)` without `LinkedHashMap::new`

|         Field         |                                 Value                                  |
|-----------------------|------------------------------------------------------------------------|
| **Pattern**           | `Collectors\.groupingBy\(`                                             |
| **Convention**        | CONVENTIONS.MD §groupingBy ordering                                    |
| **Occurrences found** | 4 (all already compliant — using 3-arg form with `LinkedHashMap::new`) |
| **Status**            | known (already in CONVENTIONS.MD) — all occurrences compliant          |

**Already compliant:**
- `FilterOptimizer.java`, `ATableQueryFactory.java`, `InMemoryTable.java`, `TableStepsGrouperByAffinity.java`

---

### 7. SLF4J log string concatenation (instead of `{}` placeholders)

|         Field         |                                     Value                                      |
|-----------------------|--------------------------------------------------------------------------------|
| **Pattern**           | `log\.(info|warn|error|debug)\(.*\+`                                           |
| **Convention**        | CONVENTIONS.MD §SLF4J placeholders                                             |
| **Occurrences found** | 1 grep hit (false positive)                                                    |
| **Status**            | known (already in CONVENTIONS.MD) — false positive; no actual violations found |

**Notes:** One grep hit in `SumAggregation.java` was a false positive — the `+` was inside a string literal format argument, not string concatenation in the log call.

---

## Pass 2 — Stepdown Rule (10 most-recently-modified files)

### 8. Stepdown Rule violation — callee defined before caller

|     Field      |                                           Value                                            |
|----------------|--------------------------------------------------------------------------------------------|
| **Convention** | CONVENTIONS.MD §Stepdown Rule (Clean Code)                                                 |
| **Rule**       | Every callee should appear directly below its first caller; entry-point methods at the top |
| **Status**     | applied (5 method-pair violations fixed across 3 files)                                    |

**Files fixed:**

**`adhoc/…/TableQueryEngine.java`**
- `makeFilterOptimizer` was defined before `bootstrap(3-arg)`, but `bootstrap(3-arg)` is the first caller of `makeFilterOptimizer`. Fixed by placing `makeFilterOptimizer` directly after `bootstrap(3-arg)`.

**`adhoc/…/DagExplainer.java`**
- `holderType(AdhocQueryId)` was at line ~270; its first caller `explain()` is the entry-point at the top of the class. Fixed by moving `holderType` to immediately after `explain()`.

**`adhoc/…/CubeQueryEngine.java`**
- `makeDagExplainer` appeared before `explainDagSteps` which calls it — violation.
- `makeDagExplainerForPerfs` appeared before `explainDagPerfs` which calls it — violation.
- Fixed by reordering to: `explainDagSteps` → `makeDagExplainer` → `explainDagPerfs` → `makeDagExplainerForPerfs`.

**No violations found in:**
- `TableQueryEngineBootstrapped.java`, `TableQueryFactory.java`, `TableStepsGrouperByAffinity.java`, `ATableQueryFactory.java`, `QueryStepsDag.java`, `ChainedInducedEvaluator.java`, `JavaStreamInducedEvaluator.java`

---

## Summary Table

| # |                  Pattern                  | Convention source |       Occurrences        |                     Status                      |
|---|-------------------------------------------|-------------------|--------------------------|-------------------------------------------------|
| 1 | JDK `List.of(...)`                        | CONVENTIONS.MD    | ~10                      | applied (7 files fixed, 4 JMH skipped)          |
| 2 | JDK `Set.of(...)`                         | CONVENTIONS.MD    | ~6                       | applied (1 fixed), 4 intentional, 2 JMH skipped |
| 3 | `new HashMap<>()`                         | CONVENTIONS.MD    | 2                        | applied (1 fixed, 1 JMH skipped)                |
| 4 | `new HashSet<>()`                         | CONVENTIONS.MD    | 0                        | no violations                                   |
| 5 | `Collectors.toMap` 2-arg                  | CONVENTIONS.MD    | 5                        | applied (3 fixed, 2 already 4-arg)              |
| 6 | `groupingBy` without `LinkedHashMap::new` | CONVENTIONS.MD    | 4                        | no violations (all already compliant)           |
| 7 | SLF4J log concatenation                   | CONVENTIONS.MD    | 0 (1 false positive)     | no violations                                   |
| 8 | Stepdown Rule                             | CONVENTIONS.MD    | 5 method-pair violations | applied (3 files fixed)                         |

---

## Notable False Positives / Edge Cases

- `[^a-zA-Z]List\.of\(` is needed (not `List\.of\(`) to avoid matching `ImmutableList.of(`
- `[^a-zA-Z]Set\.of\(` is needed to avoid matching `ImmutableSet.of(`
- `ASliceFactory.java` `Set.of(` calls are intentional class-reference registrations — **never auto-apply** this file
- JMH benchmark files under `jmh/src/main/java` may legitimately use JDK collections as benchmark subjects
- `PepperStreamHelper.toLinkedMap` is the correct helper (not `PepperStreamHelperHacked.toLinkedMap` as an older CONVENTIONS.MD draft suggested); confirmed by existing usage throughout the codebase

