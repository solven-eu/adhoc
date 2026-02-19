# CLAUDE-performance.md — AI Performance Agent Instructions

> Loaded explicitly by the performance agent prompt.
> Inherits all base conventions from the auto-loaded `CLAUDE.md`.

---

## Goal

Apply **performance optimisations** to at most 20 Java source files under the target scope.
Do **NOT** commit — the CI workflow handles that.

---

## Allowed Improvements

1. **Data-structure choices**
   - `ArrayList` → `ArrayDeque` for pure queue/stack usage (LIFO/FIFO with no random access).
   - `HashMap`/`HashSet` keyed on enums → `EnumMap`/`EnumSet`.
   - Eliminate redundant extra iterations over a collection when a single pass suffices.

2. **String / StringBuilder**
   - Replace string concatenation inside loops with `StringBuilder`.
   - Remove redundant `.toString()` calls on types already `CharSequence`-compatible.

3. **Collection API efficiency**
   - `isEmpty()` over `size() == 0`.
   - `Map.computeIfAbsent()` / `getOrDefault()` over an explicit get-then-put pattern.
   - `Collection.addAll()` over element-by-element loops.

4. **Lazy / cached computation**
   - Memoize pure, expensive methods via a field initialized on first call.
   - Avoid recomputing constants in hot paths (hoist them out of loops or into `static final` fields).

5. **JIT-friendly refactoring**
   - Split large methods that mix fast and slow paths into separate methods so the JIT can optimise each independently.
   - Reduce megamorphic call sites (more than 2 concrete types) where a straightforward refactor exists.

---

## Strict Constraints

- Only `src/main/java/**/*.java` — **no test files**, **no `pom.xml`**, **no JS/TS**.
- Do **NOT** change public API signatures or alter observable behaviour.
- Do **NOT** add new dependencies.
- Do **NOT** commit.
- Do **NOT** exceed **20 files** per run.
- After all edits, run:
  ```bash
  mvn spotless:apply --file pom.xml --batch-mode
  ```
- Also respect all conventions in the auto-loaded `CLAUDE.md` (naming, logging, collection ordering, Guava, etc.).
