# EventBus

Adhoc components publish structured events through a pluggable `IAdhocEventBus`. The bus decouples
the engine from observability concerns: log lines, query lifecycle hooks, per-step progress probes,
and any custom listener a host application wants to wire — all flow through the same single-method
abstraction. This is still early work, but the shape is stable enough that downstream projects can
already plug their own bus implementation and consume events for tracing, metrics, audit, or live
plan views.

## The interface

```java
@FunctionalInterface
public interface IAdhocEventBus {
    void post(Object event);
}
```

That is the whole contract. Implementations are free to dispatch synchronously, asynchronously,
through Guava's `EventBus`, through greenrobot, into a Spring `ApplicationEventPublisher`, into a
test-side `List<Object>` collector — anything that consumes a published object.

The engine never type-checks the bus. The only convention is that every event Adhoc publishes
implements `IAdhocEvent`:

```java
public interface IAdhocEvent {
    String getFqdn();
    IAdhocEvent withFqdn(String fqdn);
}
```

`fqdn` is the fully-qualified class name the event should be attributed to when bridged to SLF4J;
see the [FQDN section](#why-fqdn-not-logger) below for why this matters.

## Built-in event types

All events live under `eu.solven.adhoc.eventbus`.

| Event | When it fires | Carries |
|-------|---------------|---------|
| `AdhocLogEvent` | Anywhere Adhoc would have emitted a plain SLF4J log line. Has `level`, `message`, `tags`, `source`, plus `debug` / `explain` / `performance` flags. | The full message body + log level. |
| `QueryLifecycleEvent` | Start / done milestones of a top-level query. | The `IQueryPod`, tags. |
| `AdhocQueryPhaseIsCompleted` | Each main phase of `ICubeQueryEngine` finishes. | The phase name, source. |
| `QueryStepIsEvaluating` | The Cube DAG enters a given `CubeQueryStep`. | The step, the source object. |
| `QueryStepIsCompleted` | The Cube DAG finishes a `CubeQueryStep`. | The step, the number of cells produced. |
| `TableStepIsEvaluating` | The Table DAG starts a `TableQueryV4`. | The table query, the source. |
| `TableStepIsCompleted` | The Table DAG finishes a `TableQueryV4`. | The table query, the source. |

Together, the `…IsEvaluating` / `…IsCompleted` pairs give fine-grained tracking of progress through
the two DAGs the engine builds (see [CubeQueryEngine](cube-query-engine.md)). A bus subscriber can
mirror those events into a live plan view, a Prometheus histogram, or a flamegraph without the
engine knowing.

## Posting events — the SLF4J bridge

Because `AdhocLogEvent` is "I would have logged this", every Adhoc event needs to be forked to
SLF4J so logging keeps working for users who do not wire a custom bus. Instead of calling
`eventBus.post(event)` directly, use:

```java
import eu.solven.adhoc.eventbus.AdhocEventBusHelpersUnsafe;

AdhocEventBusHelpersUnsafe.logForkEventBus(eventBus, event);
```

This helper:

1. Fills in the event's `fqdn` if missing (defaulting to the helper itself — see below for the
   recommended override).
2. Writes the event to SLF4J via the `AdhocEventsFromGuavaEventBusToSfl4j` bridge — so users with
   only a logging backend still see the message.
3. Posts the event on the bus — so users with a real subscriber receive the structured event too.

The "fork to SLF4J + bus" idiom replaces the older `IAdhocEventsListener`-based approach (one
typed listener method per event class). That design was dropped — the reasons are not fully
captured in commits, but it was cumbersome to set up in tests and host projects, and getting the
FQDN propagation right across each typed callback was error-prone. The single helper is what
production code uses today.

## Wrap the bus with `safeWrapper`

When you inject an `IAdhocEventBus` into an Adhoc component (typically
`eu.solven.adhoc.engine.CubeQueryEngine#eventBus`), wrap it first:

```java
import eu.solven.adhoc.eventbus.AdhocEventBusHelpersUnsafe;

IAdhocEventBus rawBus = /* your Guava / greenrobot / Spring bus adapter */;
IAdhocEventBus safe = AdhocEventBusHelpersUnsafe.safeWrapper(rawBus);

CubeQueryEngine engine = CubeQueryEngine.builder()
        .eventBus(safe)
        // ...
        .build();
```

`safeWrapper` returns a `WrappingEventBusForSlf4jFQDN` decorator. On every `post(...)`:

- If the event implements `IAdhocEvent`, it stamps the wrapper's own class name as the event's
  FQDN and routes the event through `logForkEventBus(...)`. SLF4J emits the line; the underlying
  bus receives the event. Both happen even if Adhoc internals already called
  `logForkEventBus` — the wrapper is idempotent in the sense that re-stamping the FQDN is safe and
  the SLF4J side simply emits one line.
- If the event is not an `IAdhocEvent` (third-party event riding on the same bus), it forwards
  unchanged.

So the practical rule is: **wrap once at injection time, then forget about it.** Adhoc-internal
code can call `eventBus.post(...)` directly and the bridging still happens.

## Why FQDN, not logger

When SLF4J prints the line, the message must be attributed to the *originating Adhoc class*, not
to the bus bridge. If a logback layout uses `%logger`, the attribution always points at the
technical logger declared inside the bridge — meaningless for users debugging which step of the
engine produced the line.

`%class` solves this. Combined with SLF4J's `LocationAwareLogger.log(marker, fqcn, level, …)`
overload — which the bridge calls — and Logback's `LoggerContext#frameworkPackages`, the resolved
class lookup skips the bridge frames and lands on the real caller.

### Logback configuration

Recommended layout pattern fragment:

```
%class{15}\(%method:%line\)
```

- `%class{15}` — fully-qualified class name, abbreviated to ≤15 segments (matches the FQDN
  embedded in the event, not the technical logger).
- `%method:%line` — the method and source line inside that class.

Together they give "which Adhoc class, which method, which line emitted this" — independent of
how many bridge frames sit between the caller and the SLF4J implementation.

The bridge also exposes a helper to register the bridge frames as framework packages so Logback
skips them when computing the caller location:

```java
import eu.solven.adhoc.eventbus.AdhocEventBusHelpersUnsafe;
import ch.qos.logback.classic.LoggerContext;

LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
AdhocEventBusHelpersUnsafe.addToFrameworkPackages(context.getFrameworkPackages());
```

This adds Guava's `Subscriber` / `Dispatcher` / `EventBus`, the JDK reflection frames, and the
`AdhocEventBusHelpersUnsafe` class itself to the framework-packages list. Without this step,
`%class` may still resolve to one of those intermediate frames depending on JDK version and
classloader topology.

**Important caveat:** `%logger` will NOT respect this — it is hard-wired to the SLF4J logger
instance and ignores the FQDN argument of `LocationAwareLogger.log`. Use `%class` and accept the
small extra runtime cost of stack-walk attribution.

## A custom subscriber, end-to-end

The minimal pattern, with Guava's `EventBus`:

```java
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import eu.solven.adhoc.eventbus.AdhocEventBusHelpersUnsafe;
import eu.solven.adhoc.eventbus.IAdhocEventBus;
import eu.solven.adhoc.eventbus.QueryLifecycleEvent;
import eu.solven.adhoc.eventbus.QueryStepIsCompleted;

EventBus raw = new EventBus("adhoc");
raw.register(new Object() {
    @Subscribe
    public void onLifecycle(QueryLifecycleEvent e) { /* … */ }
    @Subscribe
    public void onStepDone(QueryStepIsCompleted e) { /* … */ }
});

IAdhocEventBus adhocBus = AdhocEventBusHelpersUnsafe.safeWrapper(raw::post);

CubeQueryEngine engine = CubeQueryEngine.builder()
        .eventBus(adhocBus)
        // ...
        .build();
```

Three things to notice:

- `raw::post` is a method reference — `IAdhocEventBus` is a functional interface so any
  `Consumer<Object>` adapts trivially.
- The `@Subscribe` methods see *real Adhoc event types*, not log strings — that is the whole point
  of having an event bus rather than parsing log lines.
- SLF4J still receives every `AdhocLogEvent` because `safeWrapper` routes through
  `logForkEventBus`. The custom subscriber adds on top of, not instead of, the log output.

## Related docs

- [CubeQueryEngine](cube-query-engine.md) — the two-DAG workflow whose progress these events expose.
- [Debug / Investigations](debug.md) — `debug` / `explain` query options that turn on the
  performance-sensitive `AdhocLogEvent` instances.
