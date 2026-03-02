# Coding Conventions — solven-eu/adhoc

> Authoritative reference for all contributors and AI agents.
> New rules are proposed by the Discovery Agent (`.claude/discovery-agent.md`) and promoted here after human review.

---

## General Principles

- Code should be easy to read and understand.
- Keep the code as simple as possible. Avoid unnecessary complexity.
- Use meaningful names for variables, functions, etc. Names should reveal intent.
- Only use comments when necessary, as they can become outdated. Strive to make the code self-explanatory.
- When comments are used, they should add useful information not readily apparent from the code itself.
- Properly handle errors and exceptions to ensure robustness. Use exceptions rather than error codes.

---

## Naming Conventions

- **Interfaces** are prefixed with `I` (e.g. `IQueryOption`, `ICubeQuery`).
- **Abstract classes** are prefixed with `A` (e.g. `AQueryEngine`).
- **Enum values** are Pascal-case (e.g. `SomeValue`, not `SOME_VALUE`).
- **Single-word concrete classes** that are not interfaces or abstract classes must be prefixed with `Adhoc` (e.g. `AdhocQuery` instead of `Query`).
- **Default implementations** for `ISomeThing` are named:
  - `SomeThing` — when the implementation is generally not customised (e.g. `CubeQueryEngine`).
  - `StandardSomeThing` — when it is expected to be frequently customised (e.g. `StandardCustomTypeManager`).

---

## Collections & Data Structures

- **Maintain insertion order**: prefer `LinkedHashSet` over `HashSet`, `LinkedHashMap` over `HashMap`.
  - Watch for `.collect(Collectors.toSet())` which does not maintain order; use `.collect(Collectors.toCollection(LinkedHashSet::new))` instead.
- **Prefer Guava immutable collections** over JDK factories:
  - `ImmutableList.of(…)` instead of `List.of(…)`
  - `ImmutableSet.of(…)` instead of `Set.of(…)`
  - Note: `List.of().contains(null)` throws; Guava equivalents behave consistently.
  - Exception: unit tests may use JDK factories for conciseness.

---

## Logging

- **SLF4J**: always use `{}` placeholders, never string concatenation.
  - Correct: `log.debug("Processing {} rows", count);`
  - Wrong:   `log.debug("Processing " + count + " rows");`

---

## Comments & Documentation

- **Inline URL references**: when writing comments, Javadoc, or YAML workflow steps, add URLs that explain *why* the code is written the way it is.
  - Preferred sources (in order): official documentation, GitHub issues/PRs, StackOverflow answers.
  - Example: `// See https://github.com/foo/bar/issues/42`
