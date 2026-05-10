# Testing

Adhoc's test pyramid is intentionally **integration-heavy at the cube level** with **localized unit tests for specific bugs and value classes**. This page documents what kinds of tests we write, where to place them, and the tooling.

## Philosophy

Adhoc is an in-memory transformation engine: the contract that matters to users is "given this measure forest, this filter and this groupBy, the cube returns these tabular rows". That contract spans the table layer, the filter layer, the measure DAG, the cube engine, and the reducer — every module at once. Most regressions are therefore most legibly captured at the **cube level** (a `CubeQuery` against an `InMemoryTable` or a `JooqTableWrapper`), because that is the surface the user actually consumes.

Two consequences:

- **The bulk of tests look like end-to-end scenarios.** They build a measure forest, feed an `InMemoryTable` (or a DuckDB-backed `JooqTableWrapper`), execute a `CubeQuery`, and assert against a `MapBasedTabularView`. Examples: `TestDagTransformator_Filtrator`, `TestDagTransformator_Unfiltrator`, every `TestDagCubeQuery_DuckDb_*` class. These tests live in `engine/cube/src/test/java/` because that is where the assembly happens, even when the bug they protect against is in a class that physically lives in `engine/table` or `libraries/measures`.
- **Localized unit tests are reserved for cases the cube-level tests cannot reach or distinguish.** Examples: `EqualsVerifier` checks on value classes, error-branch reproducers, behaviour of a specific helper method when no end-to-end query exercises it. These tests live in their home module's `src/test/java/`. We accept that this sometimes means testing implementation details — the trade-off is worth it when the bug would otherwise go silent.

The two flavours complement each other. **Cube-level tests prove the contract**; module-level unit tests **pin specific edge cases and serve as living documentation** for non-obvious branches.

### Naming convention

Two prefixes encode the distinction structurally:

- **`Test<ProductionClass>`** — a localized unit test against a single production class. One per production class is the default expectation. Lives in the same module and same package as the class under test.
- **`TestDag<Scenario>`** — an integration test that builds a measure forest, feeds a table, executes a `CubeQuery`, and asserts against the resulting `ITabularView`. **Every class extending — directly or indirectly — `ATestDagRaw`** (e.g. `ATestDagInMemory`, `ATestDagJooq`, `ATestDagDuckDb`) MUST follow this prefix. Examples: `TestDagCubeQuery_DuckDb_emptyAggregator`, `TestDagTransformator_Filtrator`, `TestDagCompositeCubesTableWrapper`.

The prefix is the structural marker for the reader: a `Test<X>` is a localized regression net for class `X`; a `TestDag<Y>` proves a cube-level contract. The two patterns are not interchangeable. The convention is also restated in [CONVENTIONS.MD § Test Coverage](https://github.com/solven-eu/adhoc/blob/master/CONVENTIONS.MD#test-coverage).

## When to write a unit test in the module under test

Write a localized unit test in the home module when **any** of the following holds:

- A bug is reproducible only by directly invoking a method, not via a `CubeQuery`. Typical: error branches that expect specific argument shapes (e.g. `produceOutputColumn` with `underlyings.size() != 1`), or methods on helper classes that are never reached by the engine but are publicly callable.
- The class is a **value type** that benefits from `EqualsVerifier.forClass(...).verify()`. One-line, comprehensive.
- The class is small and self-contained (factory, exception, parameter holder, lifecycle bean) where a direct test is shorter than the cube setup.
- The behaviour under test is **NOT covered by any cube-level test**, even after running the project-wide aggregate JaCoCo report. This is the strongest indicator: aggregate-level uncovered branches are the genuine gaps. See § Coverage attribution below.

Avoid adding a localized unit test purely to lift a per-module coverage number when the same code is already exercised by tests in another module. That is busywork that doesn't add a regression net.

## Coverage attribution: per-module vs aggregate

JaCoCo's per-module report only counts coverage from tests that ran **in the same module**. Adhoc's structure means many `engine/table` classes are extensively tested by `engine/cube` tests — the per-module report shows them as 0–60% covered, while the aggregate report (which merges every module's `jacoco.exec`) shows them at 80–100%. **This is not a bug**: it is JaCoCo's default behaviour, and it correctly reflects "tests-in-this-module vs all-tests".

The implication for adding tests:

- A class with **low local coverage but high aggregate coverage** is fine — it means the test is in another module, which is often the right place anyway (the assembly happens at the cube level). Don't duplicate.
- A class with **low aggregate coverage** is the genuine gap. That's where module-level unit tests pay off.

The CI gate is per-module today (each module's `jacoco:check` reads its own `.exec`). Threshold values are set conservatively (a few percentage points below actual local coverage) so they don't flap on minor test moves. See [CONTRIBUTING.MD § Coverage](https://github.com/solven-eu/adhoc/blob/master/CONTRIBUTING.MD#coverage) for the threshold-setting workflow and the aggregate-report invocation.

## Tooling

### Unit and integration tests (Java)

- **JUnit 5** + **AssertJ** — universal across modules. Prefer fluent AssertJ idioms (`.hasToString`, `.containsEntry`, `.isInstanceOf`, `.hasMessageContaining`).
- **Mockito** — for the rare cases where an interaction needs to be observed; most tests use real implementations or hand-rolled stubs.
- **EqualsVerifier** (`nl.jqno.equalsverifier`) — pulled in transitively via `pepper-unittest`. Use for any value class with non-trivial fields. One line per class.
- **Cucumber** — BDD tests live under `engine/cube/src/test/resources/features/`, glue code under `engine/cube/src/test/java/eu/solven/adhoc/cucumber/`, runner is `CucumberRunnerTest`. Use these for cross-cutting business scenarios that are easier to read in Gherkin than as a Java fixture.
- **DuckDB-backed integration tests** — extend `ATestDagDuckDb` for tests that need a real SQL backend. The DuckDB JDBC driver is a test-scope dependency in `engine/cube`. Use these for behaviours that depend on JOOQ-generated SQL (joins, GROUPING SETS, FILTER per aggregator).
- **`InMemoryTable`-backed tests** — extend `ATestDagInMemory` for tests where SQL is irrelevant. Cheaper, faster, sufficient for measure-DAG semantics.

### Frontend tests (JavaScript / TypeScript)

Located under `pivotable/js/`:

- **Vitest** (`vitest.config.js`) — fast unit tests for individual Vue composables and helpers. Run via `npx vitest run`.
- **Playwright** (`playwright.config.mjs`) — end-to-end browser tests under `e2e-tests/`. Run via `npx playwright test --project=chromium`. The backend is auto-started by Playwright's `webServer` config (which calls `npm run backend`). New cross-component features (DrillThrough, MeasureDag modal, query restore, …) MUST land with at least one Playwright scenario — see CLAUDE.md § UX rules.

### Benchmarks (JMH)

The `jmh/` Maven module hosts JMH micro-benchmarks. Layout mirrors the production packages: `jmh/src/main/java/eu/solven/adhoc/{compression,data,dataframe,encoding,engine,map,query,util}/`. Each benchmark is annotated with `@Benchmark`-style JMH constructs.

How to run a benchmark:

```bash
# Build the JMH benchmarks jar (annotation-processed, includes the JMH runner)
mvn package -pl jmh -am --batch-mode -DskipTests

# Run a specific benchmark class
java -jar jmh/target/benchmarks.jar BenchmarkLastLookupCache

# Run all benchmarks matching a pattern
java -jar jmh/target/benchmarks.jar -rf json -rff out.json '.*MapDictionarizer.*'
```

JMH is **not** wired into CI — benchmarks are a developer-on-demand tool for performance investigation. Add a benchmark when you want to compare two implementations of a hot path (column factory, slice factory, lookup cache, dictionary builder, etc.); leave it in the module so a future contributor can re-run it. See `BenchmarkAtomicIntegerVsLongAdder` and `BenchmarkLastLookupCache` for the canonical shapes.

### Static analysis (not tests, but related)

- **Checkstyle**, **PMD**, **SpotBugs**, **Spotless** — full suite via `mvn verify`; individual checks via `mvn pmd:check` / `mvn checkstyle:check`. The `-Pnostyle` profile skips these but keeps tests enabled — useful in iteration.
- **NullAway** (Error Prone plugin) — opt-in via `-Perrorprone`. Runs on `@NullMarked` packages only.

## Running tests

```bash
# Full reactor with all checks (slow)
mvn verify --file pom.xml --batch-mode

# Tests only, skip style/PMD/Checkstyle/Spotless
mvn test -Pnostyle --file pom.xml --batch-mode

# Single module
mvn test -Pnostyle -pl engine/cube --batch-mode

# Single test class
mvn test -Pnostyle -pl engine/cube -Dtest='TestDagTransformator_Unfiltrator' --batch-mode

# Project-wide aggregate JaCoCo report (cross-module attribution)
mvn jacoco:report-aggregate -pl :adhoc-aggregate -am --batch-mode

# Triage under-tested classes (reads the aggregate report when available)
perl scripts/check-coverage-low.pl

# Triage uncovered methods specifically (aggregate report, sorted by LOC desc)
perl scripts/check-coverage-uncovered-methods.pl 50 'engine/'

# Compare per-module thresholds vs actual coverage
perl scripts/check-coverage-constraints.pl
```

## See also

- [CONTRIBUTING.MD § Coverage](https://github.com/solven-eu/adhoc/blob/master/CONTRIBUTING.MD#coverage) — JaCoCo aggregate report, threshold-setting workflow.
- [Debug / Investigations](debug.md) — `debug(true)` / `EXPLAIN` for diagnosing query failures before writing a regression test.
- [Concurrency](concurrency.md) — thread-pool topology relevant when writing tests under `StandardQueryOptions.CONCURRENT`.

