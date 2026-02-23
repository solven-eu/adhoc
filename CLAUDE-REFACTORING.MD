# CLAUDE-REFACTORING.MD — AI Refactoring Agent Instructions

> Loaded explicitly by the refactoring agent prompt.
> Inherits all base conventions from the auto-loaded `CLAUDE.md`.

---

## Goal

Discover utility / helper methods that were **recently introduced** in the codebase and apply them
consistently across all production source files that still use the raw inline pattern the helper
was designed to encapsulate.

Do **NOT** commit — the CI workflow handles that.

---

## How to find candidates

1. Run the following to list Java files added or significantly modified in the last 60 days:
   ```bash
   git log --since="60 days ago" --diff-filter=AM --name-only --pretty=format: \
     -- '*/src/main/java/**/*.java' | sort -u
   ```
2. For each file that looks like a utility or helper (class name ending in `Helper`, `Utils`,
   `Util`, or annotated with `@UtilityClass`), read it and identify **public static methods**
   whose body is a short inline pattern (typically 1–5 lines) that callers elsewhere are likely
   to repeat verbatim.
3. For each such method, search `src/main/java` for occurrences of the raw pattern it wraps.
4. Replace each occurrence with a call to the helper method, adding the required `import` if
   not already present.

---

## Allowed improvements

Only apply a replacement when **all** of the following hold:

1. A public static helper method already exists in `src/main/java` that is **semantically
   identical** to the raw pattern (same observable behaviour, same side-effects, same exceptions).
2. The replacement is purely mechanical — no logic changes, no new decisions.
3. The import for the helper class can be added without any other file modifications.

When in doubt about equivalence, **skip the file**.

---

## Strict constraints

- Only `src/main/java/**/*.java` — **no test files**, **no `pom.xml`**, **no JS/TS**.
- Do **NOT** introduce new helper methods — that is the developer's job.
- Do **NOT** change method or constructor signatures.
- Do **NOT** alter any logic beyond the mechanical substitution.
- Do **NOT** commit.
- Do **NOT** exceed **20 files** per run.
- After all edits, run:
  ```bash
  mvn spotless:apply --file pom.xml --batch-mode
  ```
- Also respect all conventions in the auto-loaded `CLAUDE.md` (naming, logging, collection
  ordering, Guava, etc.).
