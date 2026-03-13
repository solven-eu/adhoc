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

See [`CONVENTIONS.MD`](./CONVENTIONS.MD) for the full, authoritative list. All rules there are **mandatory**.

### Visibility — prefer `protected`

This is a library designed for extensibility. Unless there is a specific reason otherwise:

- **Methods and fields should be `protected`**, not `private` or package-private, so subclasses can override or access them.
- If a method or field is intentionally `private` or package-private, add a short inline comment explaining why (e.g. `// private: implementation detail not safe to override`, `// package-private: accessed only by sibling test class`).

### Method ordering — Stepdown Rule

See `CONVENTIONS.MD`. Entry points appear first; each method is placed below its first caller. Visibility does not drive ordering.

---

## Executor Jobs

Each job below is **atomic** — run only one job per session. Do **at most 20 files** per run. After all edits, run:

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

### Discovering new jobs

New executor jobs are proposed monthly by the Discovery Agent (see `.claude/discovery-agent.md`) and promoted into this file after human review.

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
