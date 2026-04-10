# Coding Conventions — solven-eu/adhoc

> Authoritative reference for all contributors and AI agents.
> New rules are proposed by the Discovery Agent (`.claude/discovery-agent.md`) and promoted here after human review.

---

## Running Convention Checks

To scan the whole codebase in one shot:

```bash
./scripts/check-conventions.sh
```

Individual checkers can also be run directly (see **Quick scan** notes in each section below).
All checkers are informational — they never block the build.

---

## General Principles

- Code should be easy to read and understand.
- Keep the code as simple as possible. Avoid unnecessary complexity.
- Use meaningful names for variables, functions, etc. Names should reveal intent.
- Only use comments when necessary, as they can become outdated. Strive to make the code self-explanatory.
- When comments are used, they should add useful information not readily apparent from the code itself.
- Properly handle errors and exceptions to ensure robustness. Use exceptions rather than error codes.
- **Prefer positive branching**: when an `if/else` can be written with either a positive or a negated condition and both arms have the same number of lines, prefer the positive form. Negation (`!`) forces the reader to mentally invert the condition; the positive form reads more naturally.
  ```java
  // ✓ preferred — positive condition, happy path first
  if (tableQuery.isPerfectV3()) {
      return buildGroupingSets(tableQuery);
  } else {
      return buildUnionAll(tableQuery);
  }

  // ✗ avoid — negation makes the reader invert the condition
  if (!tableQuery.isPerfectV3()) {
      return buildUnionAll(tableQuery);
  } else {
      return buildGroupingSets(tableQuery);
  }
  ```
  Exception: a **guard clause** (`if (!condition) return/throw`) is preferred over wrapping the entire body in a positive block. The positive-branch rule applies only when both arms have non-trivial bodies of similar size.

- **Minimise LOC**: prefer solutions that reduce the total number of lines of code without sacrificing readability. Fewer lines mean less to read, less to maintain, and fewer places for bugs to hide. Common techniques include:
  - Extracting an intermediate variable to avoid repeating a sub-expression.
  - Delegating to an existing method instead of re-implementing its logic inline.
  - Using a guard clause / early return to eliminate a whole `else` branch.
  ```java
  // ✗ avoid — re-implements logic already in toPerfLog(V3), adds an extra helper
  return buildSelectPerfLog(v4.getAggregators(), v4.getFilter(), v4.getGroupBys());

  // ✓ preferred — delegates to the existing overload, zero extra lines
  return toPerfLog(v4.asCoveringV3());
  ```
  This is a *soft* rule: clarity always wins over line count. Do not sacrifice naming, readability, or correctness just to save lines.

- **Exclusive branches**: when two or more code paths are mutually exclusive, always use `if / else if / … / else` to make the exclusivity explicit. This applies equally to:
  - a chain of `if` blocks that each return or throw, and
  - an `if` block that returns early followed by a "default" path at the end of the method.

  The chained form communicates the exclusivity to the reader and lets the compiler/JIT skip the remaining checks once a branch is taken.
  ```java
  // ✓ preferred — communicates mutual exclusivity
  if (x instanceof Foo f) {
      return handleFoo(f);
  } else if (x instanceof Bar b) {
      return handleBar(b);
  } else {
      throw new IllegalArgumentException("unknown: " + x);
  }

  // ✗ avoid — reader must infer that the branches are exclusive
  if (x instanceof Foo f) {
      return handleFoo(f);
  }
  if (x instanceof Bar b) {
      return handleBar(b);
  }
  throw new IllegalArgumentException("unknown: " + x);

  // ✗ avoid — early return hides the else relationship
  if (condition) {
      return specialized();
  }
  return defaultPath();

  // ✓ preferred — else makes the two paths visually parallel
  if (condition) {
      return specialized();
  } else {
      return defaultPath();
  }
  ```

- **Comment every branch in complex control flow**: when a method has deep nesting (loops inside `if`/`else`, `instanceof` checks) or mid-method `return` statements, add a short `//` comment on each branch — including the implicit fall-through and the `else` that "does nothing special". The goal is to let a reader understand the intent of every path without mentally simulating the whole method. In particular:
  - Comment early `return` statements to explain *why* the method exits.
  - Comment `else` branches and fall-through paths, even when they simply continue the loop or reach the end of the method.
  - A missing comment on a branch forces the reader to re-derive the logic; a one-line comment prevents that.

---

## Naming Conventions

- **Interfaces** are prefixed with `I` (e.g. `IQueryOption`, `ICubeQuery`).
- **Abstract classes** are prefixed with `A` (e.g. `AQueryEngine`).
- **Enum values** are Pascal-case (e.g. `SomeValue`, not `SOME_VALUE`).
- **Single-word concrete classes** that are not interfaces or abstract classes must be prefixed with `Adhoc` (e.g. `AdhocQuery` instead of `Query`).
- **Default implementations** for `ISomeThing` are named:
  - `SomeThing` — when the implementation is generally not customised (e.g. `CubeQueryEngine`).
  - `StandardSomeThing` — when it is expected to be frequently customised (e.g. `StandardCustomTypeManager`).
- **Fields and local variables**: when no specific functional meaning exists, name the identifier after its type, stripping the `I`/`A` prefix and lower-casing the first letter. This is a strong preference, not a strict rule.
  - `ISliceFilter filter` (not `sliceFilter` or `myFilter`)
  - `ITableQueryEngine tableQueryEngine`
  - `IMeasure measure`
- **Map variables**: use the `keyTo` + `Value` pattern to express the key→value relationship.
  - `Map<CubeQueryStep, ICuboid> queryStepToValues`
  - `Map<String, ITableWrapper> nameToTable`
- **Optional variables**: prefix with `opt`. Applies to local variables and method parameters; fields should not be typed as `Optional<>` (following JDK guidance that `Optional` is meant only as a return type).
  - `Optional<ICuboid> optSliceToValue`
  - `Optional<CubeQueryStep> optParent`
- **Builder factory from existing instance**: use a static `edit(ExistingType)` method (not `from()`, `copy()`, or `of()`) to return a pre-populated Lombok builder.
  - `CubeQuery.edit(existingQuery)`
  - `Aggregator.edit(existingAggregator)`

---

## Collections & Data Structures

- **Immutability first**: when the resulting collection is not modified after construction, prefer Guava immutable collections over mutable ones.
  - `ImmutableSet.of(…)` instead of `new LinkedHashSet<>(…)` or `Set.of(…)`
  - `ImmutableList.of(…)` instead of `new ArrayList<>(…)` or `List.of(…)`
  - `.collect(ImmutableSet.toImmutableSet())` instead of `.collect(Collectors.toSet())` or `.collect(Collectors.toCollection(LinkedHashSet::new))`
  - Note: `List.of().contains(null)` throws; Guava equivalents behave consistently.
- **Ordered mutability when mutation is needed**: when a collection must be mutated after construction, prefer insertion-ordered types over unordered ones.
  - `LinkedHashSet` over `HashSet`
  - `LinkedHashMap` over `HashMap`
  - `.collect(Collectors.toCollection(LinkedHashSet::new))` over `.collect(Collectors.toSet())`
  - `Collectors.groupingBy(keyFn, LinkedHashMap::new, downstream)` over `Collectors.groupingBy(keyFn, downstream)` — the 2-arg overload produces a `HashMap` whose iteration order is non-deterministic across JVM runs, causing subtly unstable query results.
  - `PepperStreamHelperHacked.toLinkedMap(kFn, vFn)` over `Collectors.toMap(kFn, vFn)` — prefer the helper which enforces `LinkedHashMap` and a throwing merge function. Use the 4-arg `Collectors.toMap(..., LinkedHashMap::new)` only when `PepperStreamHelperHacked` is not on the classpath (e.g. `libraries/dataframe`).
  - **Quick scan**: `perl scripts/check-convention-linked.pl` — detects `new HashMap`, `new HashSet`, and bare `Collectors.toMap`.
- **Prefer Guava immutable collections** over JDK factories (applies even for literal constructions):
  - `ImmutableList.of(…)` instead of `List.of(…)`
  - `ImmutableSet.of(…)` instead of `Set.of(…)`
  - **Quick scan**: `perl scripts/check-convention-guava.pl` — detects `Set.of`, `List.of`, `Map.of`, `Collectors.toSet()`, `Collectors.toList()`.
- **Multi-line Map literals with 2+ entries**: `Map.of(` and `ImmutableMap.of(` that span **multiple lines** and carry 2 or more entries must be rewritten as `ImmutableMap.builder().put(…).put(…).build()` for readability. Single-line calls are acceptable regardless of entry count.
  - `ImmutableMap.of("k1", v1, "k2", v2)` — OK (single line)
  - `ImmutableMap.of("k", v)` — OK (1 entry, any layout)
  - Multi-line `Map.of(\n  "k1", v1,\n  "k2", v2\n)` — use `ImmutableMap.builder()` instead
  - **Quick scan / auto-fix**: `perl scripts/check-convention-map-builder.pl` (detect) and `perl scripts/fix-convention-map-builder.pl` (rewrite) — both operate only on multi-line calls with 2+ entries.

---

## Method Ordering — Stepdown Rule

Methods within a class are ordered so that the file reads **top-to-bottom like an outline**: a method always appears **before** the methods it calls. This is the Stepdown Rule (Clean Code, Robert C. Martin).

### Rules

1. **Entry points first.** The most important public method (e.g. `execute()`) appears near the top of the class, right after fields and constructors.
2. **Callee below caller.** Every method appears as close as possible to — and always below — its first caller. If a method is called from multiple places, place it below its first (topmost) caller.
3. **Visibility is secondary.** A `private` helper that is only called from one `public` method sits directly below that `public` method, not at the bottom of the file. Access modifiers do not drive ordering.
4. **Group by topic, not by modifier.** Methods that collaborate on the same concern are kept adjacent, regardless of whether they are `public`, `protected`, or `private`.

### Example

```java
// ✓ Stepdown order
public ITabularView execute(QueryPod queryPod) {        // entry point
    QueryStepsDag dag = makeQueryStepsDag(queryPod);   // calls (1)
    return executeDag(queryPod, dag);                  // calls (2)
}

protected QueryStepsDag makeQueryStepsDag(...) { ... } // (1) — below its caller

protected ITabularView executeDag(...) {               // (2) — below its caller
    walkUpDag(...);                                    // calls (3)
    return toTabularView(...);                         // calls (4)
}

protected void walkUpDag(...) { ... }                  // (3)
protected ITabularView toTabularView(...) { ... }      // (4)
```

### Notes

- Static constants and fields precede all methods, as usual.
- Constructors follow fields and precede instance methods.
- **Abstract methods first.** In abstract classes, `abstract` methods are placed before all concrete methods. This makes the extension contract immediately visible to readers and subclass authors without having to scan the whole file.
- This rule applies to **new or substantially refactored classes**. Existing classes are migrated incrementally — do not reorder a whole file just to satisfy this rule unless that is the explicit goal of the current task.
- **Quick scan**: `perl scripts/check-convention-stepdown.pl` — reports callee-above-caller violations.

---

## Logging

- **SLF4J**: always use `{}` placeholders, never string concatenation.
  - Correct: `log.debug("Processing {} rows", count);`
  - Wrong:   `log.debug("Processing " + count + " rows");`
  - **Quick scan**: `perl scripts/check-convention-logging.pl` — detects string concatenation in log calls.

---

## Infer / RacerD — Race-Condition Analysis

[Infer](https://fbinfer.com/) with the [RacerD](https://fbinfer.com/docs/next/checker-racerd) checker detects unprotected concurrent field accesses (e.g. a `LinkedHashMap` written from multiple threads).

### Local setup

```bash
# macOS and Linux — download the pre-built binary
# https://github.com/facebook/infer/releases
VERSION="1.2.0"
OS=$(uname -s | tr '[:upper:]' '[:lower:]' | sed 's/darwin/osx/')
ARCH=$(uname -m)   # arm64 or x86_64
curl -sSL "https://github.com/facebook/infer/releases/download/v${VERSION}/infer-${OS}-${ARCH}-v${VERSION}.tar.xz" \
  | sudo tar -C /opt -xJ
sudo mv /opt/infer-${OS}-${ARCH}-v${VERSION} /opt/infer
echo 'export PATH="/opt/infer/bin:$PATH"' >> ~/.zshrc   # or ~/.bashrc
source ~/.zshrc
```

Note: Homebrew no longer ships an `infer` formula.

### Running locally

```bash
# Whole project
./scripts/infer-racerd.sh

# Single module
./scripts/infer-racerd.sh -pl adhoc
```

Results are written to `infer-out/`. The human-readable summary is printed at the end.

### CI

A dedicated workflow (`.github/workflows/infer.yml`) runs on every push/PR to `master`. It is currently **informational only** (no build failure on findings). Once the baseline is clean, add `--fail-on-issue` to the `infer run` command in that workflow to enforce it as a gate.

---

## Test Coverage

- **Target**: ≥ 80% instruction coverage (per JaCoCo) for every production class.
- **Minimum floor**: classes below 20% are considered critically under-tested and must be addressed as a priority. They are flagged automatically in CI by `scripts/check-coverage-low.pl` (post-test, reads local JaCoCo XML) and by `scripts/check-coverage-sonar.pl` (offline, queries SonarCloud API — no build required).
- **Full SonarCloud view**: https://sonarcloud.io/component_measures?metric=uncovered_lines&view=list&id=adhoc
- **What to cover**: at minimum the nominal path and the most likely edge cases (null/empty input, boundary values, mixed types). Pure interfaces and Lombok-generated code (`@Value`, `@Builder`) do not need dedicated tests.
- **Test placement**: test classes live in the same Maven module under `src/test/java/`, named `Test<ProductionClass>` in the same package.

## Comments & Documentation

- **Inline URL references**: when writing comments, Javadoc, or YAML workflow steps, add URLs that explain *why* the code is written the way it is.
  - Preferred sources (in order): official documentation, GitHub issues/PRs, StackOverflow answers.
  - Example: `// See https://github.com/foo/bar/issues/42`
