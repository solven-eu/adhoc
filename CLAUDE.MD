# CLAUDE.md — AI Agent Instructions for solven-eu/adhoc

> This file is auto-loaded by the Claude Code CLI at startup.
> It constrains what the agent is allowed to do inside this repository.
>
> **Living document**: when the user corrects a recurring behaviour (e.g. "stop using `head`", "use this command instead"), update this file immediately so the correction applies to all future sessions.

---

## Project Overview

- **Language / runtime**: Java 21, multi-module Maven project
- **Build**: `mvn package -Pfast --file pom.xml --batch-mode` (skips slow checks; **note**: `-Pfast` also skips tests via `maven.test.skip=true`)
- **Running tests without static analysis**: `mvn test -Pnostyle --file pom.xml --batch-mode` — the `-Pnostyle` profile skips Checkstyle / PMD / SpotBugs / Spotless / license / enforcer but keeps tests enabled. Prefer this over manually passing `-Dcheckstyle.skip -Dpmd.skip -Dspotbugs.skip …`.
- **Formatting**: Spotless (Java + JS via Biome) + Prettier (JS/TS under `pivotable/js/`). For JS: **prettier runs first, Spotless/Biome second — Biome is the final authority on style**. Both are configured for tabs + 160 columns (`.prettierrc.yml` → `useTabs: true`, `.cleanthat/biome.json` → `"indentStyle": "tab"`, `"lineWidth": 160`). If you run prettier *after* spotless you may re-introduce wrap differences that spotless would otherwise rewrite — so always chain them as `npm run format && mvn spotless:apply`, never the other way.
- **Static analysis**: Checkstyle, PMD, SpotBugs — check PMD alone with `mvn pmd:check --file pom.xml --batch-mode`, Checkstyle alone with `mvn checkstyle:check --file pom.xml --batch-mode`, full suite via `mvn verify`. When reading PMD output, use the `Grep` tool to filter for `org.apache.maven.plugins.pmd.PmdViolationCheckMojo` to quickly locate violations.
- **Key libraries**: Guava, SLF4J/Logback, JUnit 5, AssertJ, Lombok

### Module structure (top-level Maven modules)

| Module | Purpose |
|--------|---------|
| `adhoc/` | Core query engine |
| `public/` | Public-facing API / interfaces |
| `pivotable/` | Spring Boot server + Vue.js front-end |
| `jmh/` | JMH micro-benchmarks |

### Local dev stack (Pivotable)

The **dev stack** is the pair of processes needed to run Pivotable locally: a Spring Boot **backend** on `:8080` and a **Vite frontend** dev server on `:5173` that proxies `/api`, `/webjars`, `/login`, `/logout` to the backend. Canonical entry points, all from `pivotable/js/`:

- `npm run backend` — backend only (Spring Boot via `mvn spring-boot:run`, profile `pivotable-unsafe` by default). Honours `$WEBMODE` (`webflux`/`webmvc`) and `$SPRING_ACTIVE_PROFILES`.
- `npm run dev_frontend` — Vite dev server only (HMR, assumes a backend is already running on `:8080`).
- `npm run dev_stack` — both together; Ctrl-C kills both. This is also what Playwright's `webServer` ultimately invokes via `npm run backend`, so `npx playwright test` auto-starts the backend if none is running.

When a task needs a running backend+frontend pair, refer to it as "the dev stack" and start it with `npm run dev_stack`.

### Logging in to a dev stack (no excuse for "I can't verify in a logged-out browser")

The default `pivotable-unsafe_fakeuser` profile registers a BASIC user whose credentials are hardcoded and whose login form is **pre-filled** by the SPA — clicking through is a 3-step flow:

1. From the home page click **You need to login**.
2. Click the provider entry **pivotable-unsafe_fakeuser** (BASIC arm).
3. Click **Login fakeUser** on the form (the username `11111111-1111-1111-1111-000000000000` and password `no_password` are already in the inputs).

Credentials, for the rare automation that needs them out-of-form:

- username: `11111111-1111-1111-1111-000000000000` (`FakeUser.ACCOUNT_ID`)
- password: `no_password`

After step 3 the navbar shows "Welcome Fake User" and the rest of the SPA is browsable. There is **no reason** to skip a UI verification on the grounds of being logged out — this works in the dev stack out of the box. See `pivotable/CONTRIBUTING.md` for more detail.

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

If there are changes in HTML/JS resources in `pivotable/js`, also run (order matters — prettier first, spotless last):

```bash
cd pivotable/js && npm run format && mvn spotless:apply
```

After this chain the tree is stable: a second `mvn spotless:apply` reports 0 changes. **Do not run `npm run format` *after* `mvn spotless:apply`** — prettier and biome use different line-wrap algorithms for expressions exceeding the 160-column limit, so running prettier last will reintroduce rewrites that spotless would normalise again on the next `mvn` run.

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
4. After all edits run `mvn spotless:apply --file pom.xml --batch-mode` then verify with `mvn test -Pnostyle --file pom.xml --batch-mode`.

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

## UX rules (Pivotable front-end)

- **Always surface asynchronous activity.** Whenever the Vue front-end performs an operation that the user is waiting on (HTTP request, query execution, login provider discovery, token refresh, export, etc.) the UI MUST make the in-flight state visible. The user should never have to guess whether something is happening. At minimum:
  - a Bootstrap spinner (`.spinner-border`, `.spinner-grow`) OR a progress bar (`.progress-bar-animated`) next to the affected element, AND
  - a short textual status (e.g. "Loading providers…", "Refreshing token…") co-located with the spinner.
  A message alone is better than nothing (see `login.js` loading state) but a spinner + message is the standard. When adding a new async action, add the loading UI in the same change — a follow-up is not acceptable.
- **Disable the triggering control while it is in-flight.** Clicking a button that is already running should not re-fire the operation. Either set `disabled` until the promise resolves, or debounce the handler.
- **Homogenize the loading idiom.** When introducing a new loading indicator, look for a sibling component already using one and reuse its pattern (class names, markup shape, text phrasing) rather than inventing a new one. The grid's progress bar (`adhoc-query-grid.js`) is the canonical example for long-running operations; `spinner-border-sm` next to a label is the canonical example for short ones.

---

## DO NOT — Prohibited actions

- Do **NOT** modify test files (`src/test/java/**`) to change test assertions or business logic. You **may** fix resource leaks, compilation errors, or other clear bugs in existing test files.
- Do **NOT** modify `pom.xml` files (no new dependencies, no version bumps).
- Do **NOT** modify anything under `.cleanthat/` (build-tool configuration managed separately).
- **Simple tasks** (Javadoc, comments, coverage tests, formatting): do **NOT** change method/constructor signatures, rename classes/methods/fields/variables, or alter business logic.
- **Complex tasks** (new features, refactoring requested by the user): signature changes, renames, and logic changes are allowed when they are part of the requested work. Prefer adding new methods/fields over modifying existing signatures; when an existing signature must change, confirm with the user first.
- Do **NOT** reorder imports (Spotless handles that).
- Do **NOT** add or remove `@Override`, `@SuppressWarnings`, or other annotations unless fixing a clear error.
- Do **NOT** commit directly to `master`. Always work on a dedicated feature branch (e.g. `git checkout -b chore/my-task`). The CI workflow handles merging and PRs.
- Do **NOT** merge pull requests, or offer to merge them. PRs are always reviewed and merged by a human through the GitHub UI.
- Do **NOT** duplicate what Checkstyle / PMD / SpotBugs / Spotless already enforce deterministically (whitespace, brace style, unused imports, etc.).
- Do **NOT** pipe Bash commands through `head`, `tail`, `grep`, `sed`, `awk`, or `echo`. Run commands directly and let the full output be captured. Use the dedicated `Grep` tool for content searches.
- Do **NOT** write a backtick character (`` ` ``) anywhere inside a Vue `template: /* HTML */ \`…\`` literal — **not even inside an `<!-- HTML comment -->`**. The template is a JavaScript template literal, and every backtick terminates the string mid-way, producing a `biome` parse error whose message (`LINE_UNDEFINED`, `Code formatting aborted due to parsing errors`) does not point at the real location. When the comment would naturally want to highlight a code fragment (a class name, a tag name, a CSS selector), use plain prose, single-quotes, or `&#96;` — never raw backticks. The same rule applies to interpolation-like fragments such as `${foo}`: they must not appear inside the template literal at all, even in comments. If you see `Code formatting aborted due to parsing errors` on a `*.js` file with a Vue template, this rule is the first thing to check.

---

## Agent efficiency rules

These rules exist because slow sessions have been observed and root-caused here.

- **Prefer `Bash` over `Task` subagents for simple CLI lookups.** Use `gh pr view`, `gh run view --log-failed`, `git log`, etc. directly as `Bash` calls (parallelised when independent). Only spawn a `Task` subagent when a search is genuinely open-ended or would flood the main context. Each subagent adds ~2–3 min of overhead.
- **Never use Python/Perl/shell scripts to edit files.** If the `Edit` tool fails (e.g. due to tab indentation — see known bug in memory), show the user the exact change to make as a code block and explain where to apply it. Do **not** fall back to `python3 -c`, `perl -i`, `awk`, or any other scripted in-place file mutation.
- **Don't re-read files already in context.** If a file was written or fully read earlier in the same session, edit it directly without a preceding `Read` call.
- **Combine Maven goals** to avoid redundant JVM startups: e.g. `mvn spotless:apply pmd:check --file pom.xml --batch-mode` instead of two separate invocations.
- **Scope Maven to the affected module** with `-pl <module>` when only one module was changed (e.g. `-pl adhoc`), rather than running the full reactor.

---

## Investigating query-engine bugs

When a `CubeQuery`-based test fails with an unexpected aggregation result (missing slice, wrong sum, etc.), the
fastest way to see what the engine is actually doing is to flip the query into debug mode and re-read the surefire
log. Two complementary techniques:

- **`CubeQuery.builder()...debug(true)...build()`** — enables `[DEBUG]` and `[EXPLAIN]` log lines that print the
  table-side rows received, the per-slice aggregator contributions, the merged step DAG, and each combinator's
  per-slice writes. The trace alone usually pinpoints which slice is being dropped/duplicated and at which step.
  Even if the test does not currently set `debug(true)`, temporarily wrapping the query with `debug(true)` (or
  the equivalent `StandardQueryOptions.DEBUG` in the options set) is the cheapest first step before reaching for
  a debugger.
- **`TabularRecordLogger`** prints `Accepted row #N: slice=... aggregates=...` for each row the table emits, and
  `TabularRecordStreamReducer` prints `Table contributes <aggregator>=<value> -> <slice>` for each per-aggregator
  contribution. Together they make it obvious whether a missing value comes from the table side, the aggregator
  side, or further downstream (combinator / join / merger).

Example: the Phase-3 sparse-keyspace bug in `UndictionarizedColumn.stream(StreamStrategy)` was diagnosed
end-to-end from a single `debug(true)` run of `TestAggregations_GroupBys#testSumOfSum_groupBy1String_notAlwaysPresent`.
The log showed the table emitting all 4 rows correctly and the per-aggregator contributions arriving as expected,
but the combinator's `Processing slice=...` lines only emitted 2 of the 3 distinct slices — proving the loss was
in the join layer, not in the table or the aggregator.
