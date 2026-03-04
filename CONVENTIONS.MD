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
  - Exception: unit tests may use JDK factories for conciseness.
- **Ordered mutability when mutation is needed**: when a collection must be mutated after construction, prefer insertion-ordered types over unordered ones.
  - `LinkedHashSet` over `HashSet`
  - `LinkedHashMap` over `HashMap`
  - `.collect(Collectors.toCollection(LinkedHashSet::new))` over `.collect(Collectors.toSet())`
- **Prefer Guava immutable collections** over JDK factories (applies even for literal constructions):
  - `ImmutableList.of(…)` instead of `List.of(…)`
  - `ImmutableSet.of(…)` instead of `Set.of(…)`

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
