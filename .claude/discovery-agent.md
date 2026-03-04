# Discovery Agent — Monthly Source Analysis

> **Schedule**: run once a month on the `master` branch (or after a significant batch of new code).
> **Role**: analyst only — no file edits, no commits, no PRs.
> **Output**: a markdown report proposing new executor jobs for human review.

---

## Goal

Read the production source code and identify **recurring, mechanical patterns** that:

1. Appear in at least 5 distinct files across the codebase.
2. Can be transformed automatically — same input always yields same output, no judgment required.
3. Carry no semantic risk (pure style, API migration, or provably equivalent rewrite).
4. Are not already covered by an existing executor job in `CLAUDE.md` or enforced by Checkstyle / PMD / SpotBugs / Spotless.

---

## Instructions

### Step 1 — Scope

Scan `src/main/java/**/*.java` across all Maven modules (`adhoc/`, `public/`, `pivotable/`, `jmh/`). Exclude `src/test/java/**`.

Use `Grep` and `Glob` to sample broadly before reading individual files. Aim for coverage, not exhaustiveness.

### Step 2 — Pattern hunting

Look for signals in these categories (non-exhaustive — new categories are welcome):

|                  Category                   |                                        Example signals                                        |
|---------------------------------------------|-----------------------------------------------------------------------------------------------|
| Deprecated API                              | JDK or library method marked `@Deprecated` still used                                         |
| Inconsistent utility use                    | `new ArrayList<>()` vs `Lists.newArrayList()`, manual null checks vs `Objects.requireNonNull` |
| Raw types                                   | `List list` instead of `List<Foo> list`                                                       |
| Inefficient idioms                          | `new ArrayList<>(Arrays.asList(...))` instead of `Lists.newArrayList(...)`                    |
| Missing `final`                             | Non-reassigned local variables or fields lacking `final`                                      |
| Boilerplate replaceable by existing helpers | Patterns that a Guava, Apache Commons, or project-internal utility already handles            |
| Inconsistent logging                        | Mixing `log.info` levels for equivalent severity events                                       |
| Repeated inline logic                       | Same 3-line pattern copy-pasted across many classes                                           |

### Step 3 — Filter ruthlessly

Discard any candidate pattern that:

- Requires understanding business intent to apply correctly.
- Would change observable behaviour (even subtly).
- Appears in fewer than 5 files.
- Is already targeted by Checkstyle, PMD, SpotBugs, or an existing CLAUDE.md job.

### Step 4 — Write the report

Output a file named `discovery-report-YYYY-MM.md` at the repo root (use the current year and month). Proposals that are approved by a human are then added to `CONVENTIONS.MD` and, if they warrant automation, promoted into a new executor job in `CLAUDE.md`.

Use the template below for each proposed rule:

---

```markdown
## Proposal: <short name>

**Pattern found**:
```java
// before
<example before code>
```

**Proposed transformation**:

```java
// after
<example after code>
```

**Prevalence**: seen in approximately N files (list up to 5 representative paths).

**Rationale**: <one sentence — why this is an improvement; link to docs/issue if relevant>.

**Risk**: None | Low | Medium — <brief justification>.

**Suggested executor job entry for CLAUDE.md**:

> ### Job: <name>
>
> <one or two bullet points describing the mechanical transformation>
>
> ```
>
> ```

---

## Hard constraints

- **Do NOT edit any source file.**
- **Do NOT commit, push, or open a PR.**
- **Do NOT propose rules that require human judgment** (naming improvements, design changes, logic rewrites).
- **Do NOT re-propose rules already listed in `CLAUDE.md`.**
- If no qualifying pattern is found, output a short report stating so — do not invent patterns to fill the report.

