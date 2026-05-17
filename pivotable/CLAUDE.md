# pivotable — local agent rules

Scoped guidance for the Pivotable backend + SPA. The root `CLAUDE.md` still applies; the rules
below kick in *specifically* when an engine-side change is being validated against Pivotable's
actual wiring (Spring beans, async query path, real registry).

## Diagnosing "I see X but no Y" in the running backend

When a user reports that a feature they expect from a recent engine-side change is missing in
their running Pivotable backend (typical shapes: "I see TableQueryV4 but no SQL", "the new
event doesn't fire", "the registry is empty after my query"), the natural temptation is to add
more unit tests. That usually proves the **unit logic** is correct without explaining why the
**wiring** doesn't reach the user — wasting time.

Default diagnostic recipe — work through these in order:

1. **Reproduce on a *dedicated* port**, not `:8080`. The user is almost certainly running their
   own backend on `:8080`. Starting yours on the same port either fails to bind or kills theirs.
   Pick `:8090` so both can coexist:

```bash
cd pivotable/js && mvn -f ../server-webflux/pom.xml -Pfast spring-boot:run \
	-Dspring-boot.run.profiles=pivotable-unsafe \
	-Dspring-boot.run.arguments=--server.port=8090
```

Start it with `run_in_background: true` so you can keep working. Use the **Monitor** tool
with `until curl -sf -o /dev/null http://localhost:8090/api/v1/public/metadata; do sleep 3;
done; echo READY` to wait for the backend to be reachable, then drive it from there.

2. **Reinstall the engine modules before starting** so the backend picks up your latest changes
   from `~/.m2/repository`. The Pivotable server depends on `:adhoc-cube` and `:adhoc-table` via
   their `SNAPSHOT` jars; without an install step, `spring-boot:run` happily uses the stale jars
   sitting in your local repo:

```bash
mvn -pl :adhoc-cube,:adhoc-table -Pnostyle -DskipTests install --batch-mode
```

3. **Stop your backend cleanly** when done — `TaskStop <task_id>`. A leaked Spring Boot process
   eats the port for the rest of the session.

## When NOT to spin up a backend — write a Java test instead

Standing up a backend is slow (8-10 s) and the cURL-with-auth dance to exercise it is brittle
(form login + CSRF + cookie jar). If the question can be answered by a Java test that mimics
Pivotable's wiring shape — Schema → CubeWrapper → executeAsync → snapshot — **prefer that**.

The shape of a Pivotable-like test in `:adhoc-cube` test sources is:

```java
BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(10_000);

CubeQueryEngine engine = CubeQueryEngine.builder()
	.eventBus(eventBus()).factories(makeFactories())
	.queryPlanRegistry(registry).build();

AdhocSchema schema = AdhocSchema.builder()
	.engine(engine)
	.env(new org.springframework.mock.env.MockEnvironment())
	.build();
schema.getRegistrer().registerTable(table);
schema.getRegistrer().registerForest(forest);
CubeWrapper cube = (CubeWrapper) schema.getRegistrer().registerCube(name, name, name);

UUID submittedUuid = UUID.randomUUID();
SubmittedQueryIdScope.runWith(submittedUuid,
	() -> cube.executeAsync(CubeQuery.builder().measure("…").build()))
	.get(15, TimeUnit.SECONDS);

QueryPlan plan = registry.snapshot(registry.findIdByUuid(submittedUuid).orElseThrow()).orElseThrow();
```

This runs in <2 s, reproduces the same registry-threading + async path the Pivotable production
wiring uses, and fails fast if the engine layer drops the registry anywhere on the chain.

## Don't accumulate e2e tests — refactor once the cause is understood

End-to-end tests that drive the full Schema → CubeWrapper → executeAsync → snapshot chain are
**valuable while diagnosing** (they reproduce the user-visible shape), but they're **expensive
to maintain** (slow, dependent on many layers). Once the root cause is found, the right move is
usually to:

1. **Pin the actual bug** with a focused unit test on the precise mechanism (e.g. on
   `QueryPodBuilder`'s `@Default` field handling, on `LiveQueryPlanSource.publishFragment`'s
   dedup contract, on `PlanFragmentScope.current()`'s no-op fallback).
2. **Delete or fold** the diagnostic e2e test if it doesn't add coverage beyond the focused
   tests + the existing e2e suite. Keeping it "just in case" leads to a slowly-growing test
   suite of overlapping scenarios.

Worked example from PR adding plan-fragment SQL leaves:
- Diagnostic e2e (`TestPivotableLike_SqlFragmentEnd2End`) reproduced "TABLE_QUERY visible but no
SQL leaf" by running the full Schema → executeAsync chain on a DuckDB-backed Jooq wrapper.
- Once the bug was localised to `QueryPodBuilder.build()` not reading the `@Default` field, the
e2e was **deleted** in favour of two 5-line tests in `TestQueryPod` pinning
`toBuilder().queryPlanRegistry(x).build()` and the default-noop contract directly. The
pre-existing `TestDagCubeQuery_DuckDB_SqlFragment` already covered the
"JooqTableWrapper publishes a SQL leaf via the pod's registry" end-to-end path.
