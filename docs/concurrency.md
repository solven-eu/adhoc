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

|                      |  `java.util.stream.Stream`   |              `IAdhocStream`              |
|----------------------|------------------------------|------------------------------------------|
| Parallelism          | `.parallel()` → ForkJoinPool | no built-in parallel split               |
| Blocking-safe        | no (FJP starvation risk)     | yes (designed for VT usage)              |
| `trySplit` semantics | eager, upfront               | n/a                                      |
| Close lifecycle      | `try-with-resources`         | explicit `close()` + `onClose(Runnable)` |

Use `IAdhocStream` everywhere a stream may originate from IO. Use `Stream` only for pure in-memory pipelines where the full dataset is already materialised.

---

## Memory strategy for growing data-structures

Allocating aggregation buffers per partition raises a tension between two constraints: avoid
costly rehashing / reallocation during a query, but also avoid pre-allocating so much memory
that parallelism causes an `OutOfMemoryError` even for small queries.

### Legacy strategy — upfront pre-allocation

The original approach allocates each column buffer at its maximum expected size upfront, using
`AdhocColumnUnsafe.getDefaultColumnCapacity()` (default: 1 000 000 entries). This prevents any
rehashing or array copy during the query at the cost of reserving 1 M-slot structures immediately.

Under sequential execution this is acceptable: one large buffer is allocated once, used, and
discarded. Under `PARTITIONED` execution however the same buffer is allocated once **per
partition**, so a query with parallelism P reserves `P × capacity` memory before a single row
is processed. For a 32-core machine with the default capacity this means 32 M slots — easily
triggering OOM on small datasets.

### Current strategy — chunked lazy growth

The replacement is a family of chunked data-structures (`ChunkedList`, `LongChunkedList`,
`DoubleChunkedList`) whose storage grows on demand rather than being pre-allocated:

- **Head chunk** — a compact array allocated lazily on the first write, sized to a small base
  (default 128 entries). A list that stays within this range never allocates anything beyond it.
- **Tail chunks** — additional chunks allocated one at a time when the head overflows. Each new
  tail chunk is twice the size of the previous one (exponential growth), bounding the number of
  allocations to `O(log n)` for a final size of `n`.

This mirrors the strategy used by Eclipse MAT's `ArrayIntBig`, and keeps the initial footprint
tiny regardless of parallelism. A query running on 32 partitions that only ever touches 100 rows
allocates 32 × 128-slot heads instead of 32 M slots.

A **linear growth** variant (fixed-size chunks, as in the original MAT approach) is under
consideration for workloads where the exponential doubling still wastes too much memory near
chunk boundaries.

### Trade-offs

|         Concern          |              Pre-allocation              |              Chunked growth               |
|--------------------------|------------------------------------------|-------------------------------------------|
| Rehash / array-copy cost | none (reserved upfront)                  | O(log n) allocations, no copy             |
| Memory for small queries | `P × capacity` regardless of actual size | proportional to actual row count          |
| Memory for large queries | 1 buffer, optimal                        | slightly fragmented across chunks         |
| `PARTITIONED` safety     | OOM risk at high parallelism             | safe — each partition starts at base size |
| Random-access speed      | single array, cache-friendly             | one indirection per tail lookup           |

The chunked approach is the current direction. Pre-allocated structures remain in place in
`MultitypeArrayColumn` and `AggregatingColumnsDistinct` while the migration is in progress.

---

## Summary table

|       Work type        |                    Pool                     |          Reason          |
|------------------------|---------------------------------------------|--------------------------|
| JDBC iteration         | `adhocMixedPool`                            | IO-bound, may block      |
| Arrow batch load       | `adhocMixedPool` (single VT, mono-threaded) | mono-threaded + IO-bound |
| Arrow slice processing | `adhocMixedPool` (per-slice VT)             | may involve mixed work   |
| HTTP / API calls       | `adhocMixedPool`                            | IO-bound                 |
| DAG orchestration      | `adhocMixedPool`                            | triggers IO-bound steps  |
| Filter optimisation    | `adhocCpuPool`                              | pure CPU                 |
| In-memory aggregation  | `adhocCpuPool`                              | pure CPU                 |
| Cache maintenance      | `maintenancePool`                           | background, daemon       |

---

## Sharding strategy for partitioned aggregation

When `PARTITIONED` is active, the engine splits aggregation into N independent partitions.
Each record is routed to a partition by a **shard key** so that all records belonging to the
same slice land in the same partition, eliminating write contention.

### Current approach: slice hashCode

The current implementation shards on `slice.hashCode() % nbPartitions` (see
`PartitioningHelpers.getPartitionIndex`). This is simple and works when the groupBy is stable
across the DAG, but **the shard assignment changes whenever the groupBy changes**.

A `Partitionor` measure, for example, computes a sub-query with a finer groupBy
(e.g. `row_index`), then aggregates up to a coarser groupBy (e.g. `l`). The slice hashCode
differs at each level, so partitioned data from the sub-query cannot be consumed
partition-by-partition at the parent level — the partitioning boundary is broken and requires
a full re-shuffle.

### Alternatives considered

|                                                 Strategy                                                 |                           Pros                           |                                                              Cons                                                              |
|----------------------------------------------------------------------------------------------------------|----------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| **Slice hashCode** (current)                                                                             | Simple, no configuration                                 | Shard key changes with groupBy; breaks partition locality across DAG levels                                                    |
| **Hardcoded columns** (e.g. always shard on column `"l"`)                                                | Stable across DAG levels if the column is always present | Column may not exist in every groupBy; requires user configuration                                                             |
| **DAG-inferred common columns** — find columns present in every GROUP BY and shard on their intersection | Automatic, stable when a common column exists            | Cumbersome; fragile if a filter pins a column to a single value (all records land in one partition); may find no common column |
| **Table-level shard key** — `ITableWrapper` declares which columns are good shard keys                   | Domain-aware, stable                                     | Requires API extension; not all tables have a natural shard key                                                                |

### Design constraints

- **Deterministic:** same slice always maps to the same partition.
- **Stable across DAG levels:** ideally a record partitioned at the table-scan level stays in the same partition when consumed by a combinator one level up, enabling partition-local processing without re-shuffling.
- **Uniform distribution:** skewed keys (e.g. a column filtered to a single value) degrade to sequential processing.
- **Cheap to compute:** evaluated once per record on the hot path.

### Re-sharding at DAG boundaries (groupBy changes)

When a DAG step changes the groupBy (e.g. `Partitionor` projects from a finer groupBy to a coarser one), the input partitions cannot be consumed directly as output partitions — the shard key changes and records must be re-distributed. Two strategies are considered:

#### Strategy A: P x P partitions

Each of P input partitions produces P output partitions (re-sharded by the new key), yielding P x P intermediate partitions. These are then merged pairwise into P final output partitions.

- **Pro:** fully parallel at every step, no contention.
- **Con:** P x P intermediate structures (e.g. 256 small columns for P=16); high memory fragmentation; complex merge step.

#### Strategy B: P partitions + re-sharding forEach (chosen direction)

Each of P input partitions produces a single output partition (mono-thread, no contention within a partition). This yields P intermediate partitions whose shard keys are based on the *input* groupBy — they are not yet sharded by the output key. A second pass (`shardingForEach`) re-distributes these P intermediate results into P final output partitions sharded by the output key.

- **Pro:** only P intermediate structures; simpler; better cache locality.
- **Con:** two sequential passes over the data (partition-local processing + re-sharding).

Strategy B is the chosen direction because:
1. Per-element work in aggregation steps is typically cheap, so the re-sharding pass adds little overhead.
2. P intermediate columns are smaller and more cache-friendly than P x P tiny columns.
3. It maps cleanly to two distinct operations: `shardedForEach` (process each input partition into its own output) and `shardingForEach` (re-distribute by the new shard key).

#### Two flavours of partitioned forEach

|     Operation     |                                                     Semantics                                                     |                        Concurrency                        |
|-------------------|-------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------|
| `shardingForEach` | Routes each element to one of P consumer threads by shard key. Elements from different partitions may interleave. | P consumer threads + 1 producer thread                    |
| `shardedForEach`  | Iterates each input partition sequentially into its own dedicated output. No cross-partition interaction.         | P independent sequential iterations (can be parallelised) |

A DAG step that changes the groupBy uses `shardedForEach` to produce P unsharded outputs, then `shardingForEach` to re-shard them into P outputs aligned with the new key.

A DAG step that preserves the groupBy (e.g. `Combinator` with the same groupBy) can consume input partitions directly with `shardedForEach` — no re-sharding needed.

### Open questions

1. Should shard keys be configurable per query, per table, or per measure?
2. Can we detect at query-planning time that a shard key will be skewed (e.g. filtered to a single value) and fall back to non-partitioned execution?
3. Is there value in supporting re-partitioning at DAG boundaries (explicit shuffle step, similar to MapReduce/Spark), rather than requiring a single stable key?
4. Can the `shardedForEach` + `shardingForEach` two-pass approach be fused into a single pass when the re-sharding function is known upfront?

