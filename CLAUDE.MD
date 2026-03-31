# CLAUDE.md — AI Agent Instructions for solven-eu/adhoc

> This file is auto-loaded by the Claude Code CLI at startup.
> It constrains what the agent is allowed to do inside this repository.
>
> **Living document**: when the user corrects a recurring behaviour (e.g. "stop using `head`", "use this command instead"), update this file immediately so the correction applies to all future sessions.

---

## Project Overview

- **Language / runtime**: Java 21, multi-module Maven project
- **Build**: `mvn package -Pfast --file pom.xml --batch-mode` (skips slow checks; **note**: `-Pfast` also skips tests via `maven.test.skip=true`)
- **Running tests without static analysis**: `mvn test -Dcheckstyle.skip=true -Dpmd.skip=true -Dspotbugs.skip=true --file pom.xml --batch-mode` — use this instead of `-Pfast` when you need to run tests while skipping only static analysis
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

See [`CONVENTIONS.MD`](./CONVENTIONS.MD) for the full, authoritative list. All rules there are **mandatory**.

### Visibility — prefer `protected`

This is a library designed for extensibility. Unless there is a specific reason otherwise:

- **Methods and fields should be `protected`**, not `private` or package-private, so subclasses can override or access them.
- If a method or field is intentionally `private` or package-private, add a short inline comment explaining why (e.g. `// private: implementation detail not safe to override`, `// package-private: accessed only by sibling test class`).

### Method ordering — Stepdown Rule

See `CONVENTIONS.MD`. Entry points appear first; each method is placed below its first caller. Visibility does not drive ordering. In abstract classes, `abstract` methods appear before concrete methods.

---

## Executor Jobs

Each job below is **atomic** — run only one job per session. Cover the whole codebase in a single pass (no file limit). After all edits, run:

```bash
mvn spotless:apply --file pom.xml --batch-mode
```

If there are changes in HTML/JS resources in `pivotable/js`, also run:

```bash
cd pivotable/js && npm run format && mvn spotless:apply
```

---

### Job: Javadoc

Add Javadoc to `public` classes and interfaces that currently have no Javadoc comment. Keep it concise and accurate. Skip test files.

---

### Job: Comment Accuracy

Fix copy-paste errors in comments — correct obviously wrong class/method names referenced in a comment. Do not rewrite or expand comments, only fix clearly wrong identifiers.

---

### Job: Low-Coverage Tests

Improve test coverage for production classes that are critically under-tested (below the 20 % floor defined in `CONVENTIONS.MD`).

**Workflow:**

1. Run the SonarCloud coverage script to get the current list of critical classes:
   ```bash
   perl scripts/check-coverage-sonar.pl
   ```
2. Filter out pure interfaces and Lombok-generated-only classes (they require no tests).
3. For each remaining class, read the source and write a `Test<ClassName>` in the same module under `src/test/java/`, in the same package. Cover at least: nominal path, null/empty inputs, and the most likely edge cases.
4. After all edits run `mvn spotless:apply --file pom.xml --batch-mode` then verify with `mvn test -Dcheckstyle.skip=true -Dpmd.skip=true -Dspotbugs.skip=true --file pom.xml --batch-mode`.

**Scope**: at most 10 classes per session to keep PRs reviewable.

---

### Discovering new jobs

New executor jobs are proposed monthly by the Discovery Agent (see `.claude/discovery-agent.md`) and promoted into this file after human review.

---

## After every change

- **Tests**: Every new class or significant behaviour change must be accompanied by a unit test. Cover at least the nominal path plus the most likely edge cases (null values, empty input, mixed types, etc.). Target ≥ 80 % instruction coverage for new files; classes below 20 % are critically under-tested — check with `perl scripts/check-coverage-sonar.pl` (offline) or `perl scripts/check-coverage-low.pl` (local JaCoCo). Test files live in the same module under `src/test/java/`.
- **CHANGES.MD**: Add a one-liner under the current `[0.0.x]` section (under `### Added`, `### Changed`, or `### Fixed` as appropriate) for every user-visible addition or change. Do **not** record coverage-only test additions — they carry no functional value for API users.
- **Final pass**: Before declaring work done, run through this checklist — do not skip any item even if the task felt simple:
  1. For every **new production class** introduced, confirm a `Test<ClassName>` exists in the same module. If it does not, write it now before responding to the user.
  2. For every **new method containing non-trivial logic** added to an existing class, confirm the existing test class covers it; add a test if not.
  3. **Javadoc vs inline comments** — use the right tool for the right audience:
     - **Javadoc** (`/** … */`) answers *what* a method does and *what* it returns — the contract visible to callers. Keep it stable: describe the observable behaviour, not the current implementation. Implementation details that are tied to a specific data structure, algorithm choice, or ordering assumption do **not** belong in Javadoc; they will become misleading if the code is refactored.
     - **Inline comments** (`// …`) explain *why* a non-obvious implementation decision was made, placed **immediately next to the code they describe**. Examples: why a stack is used instead of a queue, why a particular ordering matters, why a branch exists that looks redundant. These comments travel with the code and stay accurate after refactoring.
     - Every new method introduced by the agent must have a Javadoc comment. At minimum, one sentence describing what it does and what the return value represents. Omitting Javadoc on agent-introduced methods is a quality failure equivalent to missing tests.
     - **Preserve and adjust existing comments** — when renaming a class, method, or field, update every comment or Javadoc that references the old name. Do **not** silently delete comments because they became stale; fix them instead. A deleted comment is invisible to reviewers and permanently loses the intent it captured.
  4. **Code quality review** — re-read every touched file and ask: "would a reviewer push back on this?" Fix anything that is clearly suboptimal before responding. Applies to both production code and test code. Common signals of poor quality (not exhaustive):
     - Redundant casts (if the declared return type already satisfies the target type, drop the cast)
     - Sequence of independent `if` blocks where branches are mutually exclusive — use `if / else if / else` instead (see `CONVENTIONS.MD` Exclusive branches rule)
     - Unnecessary mutability (`ArrayList` where `ImmutableList.of(…)` suffices, `LinkedHashMap` where `ImmutableMap.builder()` suffices)
     - Dead variables, unused imports, overly verbose expressions
     - Any convention violation listed in `CONVENTIONS.MD`
     Apply all fixes in the same commit — never leave known quality issues for a follow-up.

---

## DO NOT — Prohibited actions

- Do **NOT** modify test files (`src/test/java/**`) to change test assertions or business logic. You **may** fix resource leaks, compilation errors, or other clear bugs in existing test files.
- Do **NOT** modify `pom.xml` files (no new dependencies, no version bumps).
- Do **NOT** modify anything under `.cleanthat/` (build-tool configuration managed separately).
- Do **NOT** modify anything under `pivotable/js/` (JS/TS handled by Prettier separately).
- **Simple tasks** (Javadoc, comments, coverage tests, formatting): do **NOT** change method/constructor signatures, rename classes/methods/fields/variables, or alter business logic.
- **Complex tasks** (new features, refactoring requested by the user): signature changes, renames, and logic changes are allowed when they are part of the requested work. Prefer adding new methods/fields over modifying existing signatures; when an existing signature must change, confirm with the user first.
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
- **Never use Python/Perl/shell scripts to edit files.** If the `Edit` tool fails (e.g. due to tab indentation — see known bug in memory), show the user the exact change to make as a code block and explain where to apply it. Do **not** fall back to `python3 -c`, `perl -i`, `awk`, or any other scripted in-place file mutation.
- **Don't re-read files already in context.** If a file was written or fully read earlier in the same session, edit it directly without a preceding `Read` call.
- **Combine Maven goals** to avoid redundant JVM startups: e.g. `mvn spotless:apply pmd:check --file pom.xml --batch-mode` instead of two separate invocations.
- **Scope Maven to the affected module** with `-pl <module>` when only one module was changed (e.g. `-pl adhoc`), rather than running the full reactor.
