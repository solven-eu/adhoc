# CLAUDE-REFACTORING.MD — AI Refactoring Agent Instructions

> Loaded explicitly by the refactoring agent prompt.
> Inherits all base conventions from the auto-loaded `CLAUDE.md`.

---

## Goal

Find inline patterns in production source code that could be replaced by an equivalent,
already-existing method — from any of these sources — and apply those replacements mechanically:

- **Own static helpers** — `@UtilityClass` / `*Helper` / `*Utils` classes in `src/main/java`
- **Well-known dependency utilities** — Guava, Pepper, etc.
- **Non-static methods on the same class** — e.g. a method that already exists on `this` but the
  body duplicates its logic inline
- **Inherited methods from a parent abstract class** — e.g. an abstract base class already
  provides a helper that a subclass re-implements inline

Do **NOT** commit — the CI workflow handles that.

---

## How to find candidates

**Start from duplication, not from helpers.** Read production source files and look for code
fragments that appear more than once — across files, within the same file, or between a class
and its parent. Once a duplicated pattern is identified, check whether an existing method already
encapsulates it. If yes, replace the duplicate with a call to that method.

Possible sources of the replacement method (check in this order):

1. **Same class** — a method on `this` that another method in the same class duplicates inline
   instead of delegating.
2. **Parent abstract class** — a non-abstract method inherited from the class hierarchy (classes
   starting with `A` follow the project's abstract-class convention) that a subclass re-implements
   inline rather than calling via `super` or direct invocation.
3. **Own static helpers** — a public static method in any `@UtilityClass` / `*Helper` / `*Utils`
   class in `src/main/java`.
4. **Well-known dependency utilities** already on the classpath — for example:
   - **Guava**: `Strings` (nullToEmpty, emptyToNull, isNullOrEmpty), `MoreObjects.firstNonNull`,
     `Preconditions` (checkNotNull, checkArgument), `Iterables`, `Maps`, `Sets`, `Lists`
   - **Pepper**: `Pepper*Helper` classes — discover the available ones from existing imports in
     the codebase

A good discovery workflow:
1. Pick a source file to read.
2. Identify any short block (1–6 lines) that looks like it might be duplicated. Single-line
   expressions are valid candidates (e.g. `x == null ? y : x` → `MoreObjects.firstNonNull(x, y)`,
   `x == null ? "" : x` → `Strings.nullToEmpty(x)`).
3. `grep` for the key tokens of that block across `src/main/java` to confirm duplication.
4. For each confirmed duplicate, search the sources above for an existing method that is
   semantically identical and can replace it.

---

## Allowed improvements

Only apply a replacement when **all** of the following hold:

1. A method already exists (static or instance, own code or dependency) that is **semantically
   identical** to the raw pattern (same observable behaviour, same side-effects, same exceptions).
2. The replacement is purely mechanical — no logic changes, no new decisions.
3. For static imports: the required `import` can be added without any other file modifications.
4. For instance/inherited methods: the method is accessible without changing visibility or
   adding casts.
5. The dependency providing the method (if external) is already present in the module's `pom.xml`
   (do **NOT** add new dependencies).

When in doubt about equivalence, **skip the file**.

---

## Strict constraints

- Only `src/main/java/**/*.java` — **no test files**, **no `pom.xml`**, **no JS/TS**.
- Do **NOT** introduce new helper methods — that is the developer's job.
- Do **NOT** change method or constructor signatures.
- Do **NOT** alter any logic beyond the mechanical substitution.
- Do **NOT** exceed **20 files** per run.
- After all edits, run:
  ```bash
  mvn spotless:apply --file pom.xml --batch-mode
  ```
- Also respect all conventions in the auto-loaded `CLAUDE.md` (naming, logging, collection
  ordering, Guava, etc.).
