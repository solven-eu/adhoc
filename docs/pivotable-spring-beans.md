# Pivotable Spring beans — overriding caveats

Pivotable's Spring auto-configuration (`AdhocAutoConfiguration` in `pivotable-server-core`) registers a handful of
singleton beans that are consumed by **multiple** parts of the stack — typically a writer (the cube engine) on one
side and a reader (an HTTP handler, the UI, a subscriber) on the other. Spring guarantees one instance per bean
type, so as long as every consumer goes through `@Autowired` / constructor injection, the wiring is correct.

The trap appears when a downstream project overrides a *higher-level* bean (for example to inject custom factories
or a domain-specific schema) and constructs its inner collaborators **by hand**, ignoring the auto-configured
singletons:

```java
@Bean
public ICubeQueryEngine myEngine(IAdhocEventBus eventBus, IAdhocFactories factories) {
    return CubeQueryEngine.builder()
            .eventBus(eventBus)
            .factories(factories)
            // ← no .queryPlanRegistry(...) — falls back to a noop instance
            .build();
}
```

Spring still creates the auto-configured `IQueryPlanRegistry`. The HTTP plan handlers happily inject it (empty,
because nothing ever wrote to it). The engine writes to its private noop registry that nobody can read. The UI
shows "no plan available" for every query.

The same shape applies to every bean below: if you override a higher-level bean, **inject the auto-configured
collaborator rather than constructing your own.**

## Singleton-shared beans

| Bean                  | Writer                                | Reader / subscriber                                        | What breaks if you don't share it                                                                  |
|-----------------------|---------------------------------------|------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| `IQueryPlanRegistry`  | `CubeQueryEngine` (via `QueryPod`)    | `PivotablePlanController` / `PivotablePlanHandler`         | `/api/v1/cubes/queries/{id}/plan/*` endpoints return empty; UI Live View shows nothing             |
| `IQueryStepCache`     | `StandardQueryPreparator`             | `CubeQueryEngine` (per-step lookup)                        | Cache hit-rate drops to 0; every query recomputes from scratch                                     |
| `IAdhocEventBus`      | `CubeQueryEngine`, table-layer hooks  | `AdhocEventsFromGuavaEventBusToSfl4j`, custom subscribers  | Engine events (EXPLAIN, perf, fragments) never reach the SLF4J sink or any custom subscriber       |
| `IImplicitOptions`    | `StandardQueryPreparator`             | Per-query options merge                                    | Per-request defaults (e.g. `debug=true` from request header) silently ignored                      |
| `IImplicitFilter`     | `StandardQueryPreparator`             | Per-query filter merge                                     | Authorization filters (rights management) silently dropped — security regression                   |

## Higher-level beans that aggregate the above

These beans take the singletons as constructor arguments. If you override **any of them**, inject the
auto-configured singletons rather than building fresh:

- `IQueryPreparator` — wires `IQueryStepCache`, `IQueryPlanRegistry`, `IImplicitOptions`, `IImplicitFilter`.
- `ICubeQueryEngine` — wires `IAdhocEventBus`, `IAdhocFactories`.
- `AdhocSchema` — wires the preparator and the engine; cube registration goes through this.

Concrete shape that works:

```java
@Bean
public AdhocSchema mySchema(ICubeQueryEngine engine,           // ← auto-configured, do not rebuild
                            IQueryPreparator queryPreparator,  // ← auto-configured, do not rebuild
                            Environment env) {
    return AdhocSchema.builder()
            .engine(engine)
            .queryPreparator(queryPreparator)
            .env(env)
            .build();
}
```

If you genuinely need a different `IQueryPlanRegistry` (e.g. a remote variant), provide your own
`@Bean IQueryPlanRegistry` — the auto-config drops out via `@ConditionalOnMissingBean(IQueryPlanRegistry.class)`,
and Spring then injects YOUR instance into the preparator AND the HTTP handlers. That path works precisely
because it goes through Spring's container, which is exactly the property we lose when constructing by hand.

## Quick sanity-check

If you suspect a desync, add a temporary log in your custom bean:

```java
log.info("Wiring engine with IQueryPlanRegistry@{}", System.identityHashCode(queryPlanRegistry));
```

…and a matching log in the handler (or just inspect the bean in IntelliJ's Spring view). The `identityHashCode`
values must match between the engine and the handler. If they differ, you have two instances and the wiring is
the problem.
