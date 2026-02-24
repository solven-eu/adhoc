# CLAUDE-COVERAGE.MD — AI Coverage Agent Instructions

> Loaded explicitly by the coverage agent prompt.
> Inherits all base conventions from the auto-loaded `CLAUDE.md`, with the
> overrides noted below.

---

## Goal

Improve **test coverage** for at most 20 Java source files by writing or extending
JUnit 5 tests, guided by the JaCoCo XML reports produced in this run.

Do **NOT** commit — the CI workflow handles that.

---

## How to find coverage gaps

1. Read the JaCoCo XML reports at `./**/target/site/jacoco/jacoco.xml`.
   Each `<class>` element contains `<counter type="BRANCH" missed="…" covered="…"/>` and
   `<counter type="INSTRUCTION" missed="…" covered="…"/>` counters at class level.
2. Compute `covered / (missed + covered)` for BRANCH and INSTRUCTION per class.
3. Rank classes by the lowest ratio. Focus exclusively on classes in the modules tracked
   by SonarCloud (the ones with meaningful coverage thresholds):
   - `adhoc/src/main/java`
   - `public/src/main/java`
   - `libraries/fsst/src/main/java`
   - `libraries/encoding/src/main/java`
4. Choose the **top 20 classes** with the biggest absolute gap (missed counter) —
   prefer classes with many missed branches over many missed instructions, since
   branch coverage is the harder metric to satisfy.

---

## Allowed Improvements

1. **Write new test classes** in the corresponding `src/test/java` tree.
   Mirror the package of the class under test.
   Use the naming convention `<ClassName>Test.java`.
2. **Add test methods** to an existing `*Test.java` file when one already exists for
   the target class.
3. Tests must use **JUnit 5** (`@Test`, `@ParameterizedTest`, etc.) and **AssertJ**
   assertions (`assertThat(…).…`).
4. Mock external dependencies with **Mockito** only if it is already used elsewhere in
   the same module's test tree; otherwise prefer a real implementation or a simple stub.

---

## Strict Constraints (override / supplement to `CLAUDE.md`)

- **Override**: You MAY create new test files and add test methods to existing test files
  in `src/test/java/**`. You must NOT delete or modify existing test logic.
- Only `src/test/java/**/*.java` — **no production code**, **no `pom.xml`**, **no JS/TS**.
- Do **NOT** write trivial tests (pure getter/setter coverage, tautological assertions).
  Every test must assert a meaningful behaviour or invariant.
- Do **NOT** add new dependencies (`pom.xml` is off-limits).
- Do **NOT** exceed **20 files** per run (test files count toward the limit).
- Do **NOT** commit.
- After all edits, run:
  ```bash
  mvn spotless:apply --file pom.xml --batch-mode
  ```
- Also respect all conventions in the auto-loaded `CLAUDE.md` (naming, logging,
  collection ordering, Guava, etc.) when writing test code.
