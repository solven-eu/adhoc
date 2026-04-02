# Concurrency model

This document describes the thread-pool topology used by Adhoc, the rules governing which pool is used where, and the rationale behind those rules.

---

## Thread pools

All pools are declared in `AdhocUnsafe` and are application-scoped (one instance per ClassLoader).

### `adhocMixedPool` — virtual-thread executor

```java
AdhocUnsafe.adhocMixedPool
// Executors.newVirtualThreadPerTaskExecutor() wrapped in a ListeningExecutorService
```

**Purpose:** general-purpose pool for work that may block — JDBC queries, Arrow batch loading, API calls, cache refreshes, DAG step orchestration.  
**Thread type:** Java virtual threads (Project Loom). Each submitted task gets its own VT; blocking a VT parks it cheaply without consuming a platform thread.  
**Sizing:** unbounded — the JVM schedules VTs onto a small set of carrier threads.

### `adhocCpuPool` — ForkJoinPool

```java
AdhocUnsafe.adhocCpuPool
// new ForkJoinPool(parallelism, NamingForkJoinWorkerThreadFactory("adhoc-cpu-"), null, asyncMode=false)
```

**Purpose:** strictly CPU-bound work — filter optimisation, DAG graph traversal, slice comparisons, in-memory aggregations.  
**Thread type:** platform threads managed by a ForkJoinPool.  
**Sizing:** `adhoc.parallelism` system property, defaulting to `availableProcessors × 2` (to absorb brief stalls on internal locks).  
**`asyncMode=false`:** stack (LIFO) scheduling is preferred over queue (FIFO) for DAG traversal, since recently pushed sub-tasks are more likely to share cache with their parent.

### `maintenancePool` — cached platform-thread pool

```java
AdhocUnsafe.maintenancePool
// Executors.newCachedThreadPool(..., daemon=true)
```

**Purpose:** background maintenance — `CacheBuilder.refreshAfterWrite`, periodic housekeeping.  
**Daemon threads:** the JVM will not wait for these threads on shutdown.

---

## Pool selection rules

### Mixed-bound tasks use `adhocMixedPool`

Submit to `adhocMixedPool` whenever a task may block, even briefly:

- JDBC query execution and result-set iteration
- Arrow batch loading (`ArrowReader.loadNextBatch()`)
- HTTP / RPC calls (BigQuery, ClickHouse, Redshift, …)
- Loading data from disk
- Any `ITableWrapper.openReader(…)` call
- DAG step execution in `DagCompletableExecutor` when steps trigger table queries

The virtual-thread executor never blocks a carrier thread on IO, so hundreds of concurrent table queries can be in flight with no thread exhaustion.

### CPU-bound tasks use `adhocCpuPool`

Submit to `adhocCpuPool` only when:

- All work within the task is pure computation (no IO, no locks on external resources)
- The task does not call into `adhocMixedPool` (see cross-pool rule below)

Examples: `ISliceFilter` predicate evaluation, combinatorial rewriting, in-memory sort/merge.

### Cross-pool call direction: mixed → cpu only

> **Mixed-bound tasks may submit sub-tasks to `adhocCpuPool`.**  
> **CPU-bound tasks must never submit to or block on `adhocMixedPool`.**

Rationale: a FJP carrier thread that blocks waiting for a VT future will hold a platform thread and reduce the effective parallelism of the CPU pool — potentially deadlocking if all FJP threads are waiting. VTs, by contrast, can park cheaply while waiting for CPU futures to complete.

---

## Why `Stream.parallel()` is banned for IO-bound work

`Stream.parallel()` submits tasks to the JVM common ForkJoinPool (or whichever FJP is the current pool context). This is wrong for IO-bound sources for two reasons:

1. **FJP is not blocking-friendly.** A VT submitted to a FJP runs on a carrier thread; if it blocks (JDBC, Arrow read), it holds a carrier and shrinks the pool's effective parallelism.

2. **`Spliterator.trySplit()` is called eagerly.** `Stream` implementations split the source upfront to distribute work before any data is available. Arrow's `ArrowReader` is mono-threaded and sequential: batches are only available after `loadNextBatch()` returns. A `Spliterator` wrapping it cannot split early and the stream silently degrades to sequential.

**Correct approach for Arrow:** load batches mono-threadedly on a single VT; once a batch is loaded, slice it and submit each slice as an independent task to `adhocMixedPool`. This is exactly what `ArrowPojoStreamer` does.

**Correct approach for JDBC:** iterate the `ResultSet` mono-threadedly on a VT; push each row or page into `IAdhocStream` pipeline which runs on the same VT.

---

## `IAdhocStream` vs `Stream`

`IAdhocStream<T>` is Adhoc's own streaming abstraction, intentionally decoupled from `java.util.stream.Stream`. Key differences:

| | `java.util.stream.Stream` | `IAdhocStream` |
|---|---|---|
| Parallelism | `.parallel()` → ForkJoinPool | no built-in parallel split |
| Blocking-safe | no (FJP starvation risk) | yes (designed for VT usage) |
| `trySplit` semantics | eager, upfront | n/a |
| Close lifecycle | `try-with-resources` | explicit `close()` + `onClose(Runnable)` |

Use `IAdhocStream` everywhere a stream may originate from IO. Use `Stream` only for pure in-memory pipelines where the full dataset is already materialised.

---

## Summary table

| Work type | Pool | Reason |
|---|---|---|
| JDBC iteration | `adhocMixedPool` | IO-bound, may block |
| Arrow batch load | `adhocMixedPool` (single VT, mono-threaded) | mono-threaded + IO-bound |
| Arrow slice processing | `adhocMixedPool` (per-slice VT) | may involve mixed work |
| HTTP / API calls | `adhocMixedPool` | IO-bound |
| DAG orchestration | `adhocMixedPool` | triggers IO-bound steps |
| Filter optimisation | `adhocCpuPool` | pure CPU |
| In-memory aggregation | `adhocCpuPool` | pure CPU |
| Cache maintenance | `maintenancePool` | background, daemon |
