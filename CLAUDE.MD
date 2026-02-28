# CLAUDE.md — AI Agent Instructions for solven-eu/adhoc

> This file is auto-loaded by the Claude Code CLI at startup.
> It constrains what the agent is allowed to do inside this repository.
>
> **Living document**: when the user corrects a recurring behaviour (e.g. "stop using `head`", "use this command instead"), update this file immediately so the correction applies to all future sessions.

---

## Project Overview

- **Language / runtime**: Java 21, multi-module Maven project
- **Build**: `mvn package -Pfast --file pom.xml --batch-mode` (skips slow checks)
- **Formatting**: Spotless (Java) + Prettier (JS/TS under `pivotable/js/`)
- **Static analysis**: Checkstyle, PMD, SpotBugs — check PMD alone with `mvn pmd:check --file pom.xml --batch-mode`, Checkstyle alone with `mvn checkstyle:check --file pom.xml --batch-mode`, full suite via `mvn verify`. When reading PMD output, use the `Grep` tool to filter for `org.apache.maven.plugins.pmd.PmdViolationCheckMojo` to quickly locate violations.
- **Key libraries**: Guava, SLF4J/Logback, JUnit 5, AssertJ, Lombok

### Module structure (top-level Maven modules)

| Module | Purpose |
|--------|---------|
| `adhoc/` | Core query engine |
| `public/` | Public-facing API / interfaces |
| `pivotable/` | Spring Boot server + Vue.js front-end |
| `jmh/` | JMH micro-benchmarks |

---

## Coding Conventions

These are **mandatory** — follow them exactly:

- **Interfaces** are prefixed with `I` (e.g. `IQueryOption`, `ICubeQuery`).
- **Abstract classes** are prefixed with `A` (e.g. `AQueryEngine`).
- **Enum values** are Pascal-case (e.g. `SomeValue`, not `SOME_VALUE`).
- **Single-word concrete classes** that are not interfaces or abstract classes must be prefixed with `Adhoc` (e.g. `AdhocQuery` instead of `Query`).
- **Default implementations** for `ISomeThing` are named `SomeThing` when rarely customised, or `StandardSomeThing` when frequently customised.
- Prefer data structures that **maintain insertion order**: use `LinkedHashSet` instead of `HashSet`, `LinkedHashMap` instead of `HashMap`. Watch for `.collect(Collectors.toSet())` which does not maintain order.
- Prefer **Guava immutable collections** over JDK wrappers: use `ImmutableList.of(…)` / `ImmutableSet.of(…)` instead of `List.of(…)` / `Set.of(…)`. Note: `List.of().contains(null)` throws; Guava equivalents behave consistently.
- **SLF4J logging**: always use `{}` placeholders, never string concatenation.
  - Correct: `log.debug("Processing {} rows", count);`
  - Wrong:   `log.debug("Processing " + count + " rows");`
- **Inline URL references**: When writing comments, Javadoc, or YAML workflow steps, add URLs that explain *why* the code is written the way it is. Preferred sources (in order): official documentation, GitHub issues/PRs, StackOverflow answers. Example: `// See https://github.com/foo/bar/issues/42`

---

## DO — Allowed improvements

Limit yourself to the following categories. Do **at most 20 files** per run.

1. **Add Javadoc** to `public` classes and interfaces that currently have no Javadoc comment. Keep it concise and accurate.
2. **Replace unordered collections** — `HashSet` → `LinkedHashSet`, `HashMap` → `LinkedHashMap`.
3. **Replace JDK collection factories** — `List.of(…)` → `ImmutableList.of(…)`, `Set.of(…)` → `ImmutableSet.of(…)` where semantically equivalent, except in unit-tests where conciseness primes.
4. **Fix SLF4J concatenation** — convert `log.xxx("text" + var)` to `log.xxx("text {}", var)`.
5. **Fix copy-paste errors in comments** — correcting obviously wrong class/method names referenced in a comment.

After all edits, run:

```bash
mvn spotless:apply --file pom.xml --batch-mode
```

If there is changes in HTML/JS resources in `pivotable/js`, run:

```bash
cd pivotable/js && npm run format && mvn spotless:apply
```

---

## DO NOT — Prohibited actions

- Do **NOT** modify test files (`src/test/java/**`).
- Do **NOT** modify `pom.xml` files (no new dependencies, no version bumps).
- Do **NOT** modify anything under `.cleanthat/` (build-tool configuration managed separately).
- Do **NOT** modify anything under `pivotable/js/` (JS/TS handled by Prettier separately).
- Do **NOT** change method or constructor **signatures**.
- Do **NOT** rename classes, methods, fields, or variables.
- Do **NOT** alter business logic or algorithm behaviour.
- Do **NOT** reorder imports (Spotless handles that).
- Do **NOT** add or remove `@Override`, `@SuppressWarnings`, or other annotations unless fixing a clear error.
- Do **NOT** commit directly to `master`. Always work on a dedicated feature branch (e.g. `git checkout -b chore/my-task`). The CI workflow handles merging and PRs.
- Do **NOT** merge pull requests, or offer to merge them. PRs are always reviewed and merged by a human through the GitHub UI.
- Do **NOT** duplicate what Checkstyle / PMD / SpotBugs / Spotless already enforce deterministically (whitespace, brace style, unused imports, etc.).
- Do **NOT** pipe Bash commands through `head`, `tail`, `grep`, `sed`, `awk`, or `echo`. Run commands directly and let the full output be captured. Use the dedicated `Grep` tool for content searches.

---

## Agent efficiency rules

These rules exist because slow sessions have been observed and root-caused here.

- **Prefer `Bash` over `Task` subagents for simple CLI lookups.** Use `gh pr view`, `gh run view --log-failed`, `git log`, etc. directly as `Bash` calls (parallelised when independent). Only spawn a `Task` subagent when a search is genuinely open-ended or would flood the main context. Each subagent adds ~2–3 min of overhead.
- **Don't re-read files already in context.** If a file was written or fully read earlier in the same session, edit it directly without a preceding `Read` call.
- **Combine Maven goals** to avoid redundant JVM startups: e.g. `mvn spotless:apply pmd:check --file pom.xml --batch-mode` instead of two separate invocations.
- **Scope Maven to the affected module** with `-pl <module>` when only one module was changed (e.g. `-pl adhoc`), rather than running the full reactor.
