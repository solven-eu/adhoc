# Adhoc query-engine optimizations

This document collects design notes for the optimizations layered on top of the basic DAG of `CubeQueryStep`s. It is meant for engine contributors — application authors do not need to read it.

## Trade-off philosophy

Adhoc is an OLAP engine: a single `CubeQueryStep` typically materialises a large cuboid (thousands to millions of cells). Most optimization passes therefore optimise for the **large-cuboid case** at the cost of a small overhead on the **small-cuboid case**.

Concretely, when adding a new optimization we accept that:

- Trivial queries (sub-millisecond, head-only cuboids, no chains) may pay a small fixed cost — typically a graph traversal or a counter setup — that the optimization itself can never amortise on such tiny work.
- Complex queries (multi-million-cell cuboids, long combinator chains, wide groupBys) save proportionally large amounts of memory or CPU.

This asymmetric trade-off is intentional: a trivial query that goes from 200 µs to 220 µs is invisible to the user, while a complex query that goes from 30 s to 8 s — or from OOM to "fits in 512 MB" — is decisive. The user noticed the slow case; nobody is timing the fast case.

When proposing a new pass, sanity-check it against this rule:

> Is the overhead small enough that the user will never notice it on trivial queries? Is the saving large enough that the user *will* notice it on the queries they actually care about?

Both must be "yes". If the overhead is non-trivial relative to small queries, the pass needs a gating heuristic (cuboid-size threshold, chain-length threshold, etc.).

## Cuboid pruning during `walkUpDag`

See `CubeQueryEngine.walkUpDag` and `pruneUnderlyings`. Each `CubeQueryStep` carries a per-instance "remaining-consumers" counter; once a step's counter hits zero and the step is not user-requested, its cuboid is removed from the in-flight `queryStepToValues` map. Without this, a long chain of same-cardinality combinators accumulates every intermediate cuboid in heap and OOMs on large groupBy cardinalities — see `TestDagCubeQuery_LongChainPruning`.

## Append cache on `Chunked*List`

See `ChunkedList`, `ChunkedLongList`, `ChunkedDoubleList`. Each holds an `appendChunk` reference + `appendChunkBase` int; consecutive appends stay on a fast path that skips the per-cell `(adjusted, k, offset)` arithmetic (and the `numberOfLeadingZeros` intrinsic call) that was otherwise paid twice per append. `set()` preserves the cache (no size change); mid-list inserts and removals invalidate it. `MultitypeArray.add()` is the matching specialised entry point at the higher layer, routing each `IValueReceiver` callback to the underlying list's pure-append path.

## Pluggable DAG optimizer

`QueryStepsDagBuilder.registerRootWithDescendants` runs an `IQueryStepsDagOptimizer` after the DAG is built and before `getQueryDag()` returns. The interface lives in `eu.solven.adhoc.engine.optimizer` and exposes a single mutate-in-place entry point. Implementations available:

- `NoopQueryStepsDagOptimizer` — for tests / projects that want the raw DAG.
- `PartitionorToCombinatorOptimizer` — rewrites a `Partitionor`-measure step as an equivalent `Combinator`-measure step when the step's groupBy already covers the Partitionor's own groupBy (so the per-partition aggregation is a no-op and the engine routes underlyings at the same granularity a Combinator would). Wired first in the default composite so the resulting Combinators participate in subgraph folding. Preserves each consumer's outgoing-edge order during rewrite — the engine uses `multigraph.outgoingEdgesOf(consumer)` order to map cuboids to `Combinator.underlyings[i]` positionally, so naively `removeVertex` + re-`addEdge` would silently swap the order and corrupt the combination's input order (we initially shipped this bug — see `TestDagCubeQuery_NullPropagation`). The order-preserving swap is centralised in `OptimizerHelpers.replaceStepMeasure`, shared with the Filtrator and Unfiltrator rewrites.
- `FiltratorToCombinatorOptimizer` — rewrites a `Filtrator`-measure step as a passthrough `Combinator(COALESCE)` when the Filtrator's filter is already implied by the step filter (`step.filter AND filtrator.filter == step.filter` after optimization). In that case the Filtrator would query its underlying at the same filter as the step itself, and per-cell behaves exactly like a `CoalesceCombination` on its single underlying.
- `UnfiltratorToCombinatorOptimizer` — rewrites an `Unfiltrator`-measure step as a passthrough `Combinator(COALESCE)` when the Unfiltrator's filter editor would leave the step filter unchanged. Concretely, `Mode.Suppress` is a no-op when none of the listed columns appear in the step filter, and `Mode.Retain` is a no-op when the step filter only constrains listed columns. The check uses `SimpleFilterEditor.suppressColumn` / `retainsColumns` on the step filter and compares the result with the original via `.equals`.
- `FoldCombinatorSubgraphsOptimizer(minChainLength, IComposedNameStrategy)` — folds any connected subgraph of single-consumer `Combinator` steps (chain or tree) into one `ComposedCombination`-backed step (registered under the short key `ComposedCombination.KEY = "COMPOSED"`, so EXPLAIN renders `Combinator[COMPOSED]`). Default `minChainLength = 2`; the default-constructor optimizer is the one wired into the builder. The fused-step name is built by a pluggable `IComposedNameStrategy` whose default preserves each internal's `name[combinationKey]` (e.g. `ratio_postcheck[DIVIDE] ∘ whole[COALESCE]`) — so the EXPLAIN line tells you both what fused and which operations were folded.
- `CompositeQueryStepsDagOptimizer(IQueryStepsDagOptimizer...)` — sequences passes; the default composite is `PartitionorToCombinatorOptimizer` then `FiltratorToCombinatorOptimizer` then `UnfiltratorToCombinatorOptimizer` then `FoldCombinatorSubgraphsOptimizer`, so every per-measure rewrite into a passthrough Combinator participates in subgraph folding before the fold pass runs.

### Why Shiftor is not rewritten

`Shiftor` is intentionally absent from the default chain, even with an identity filter editor. `ShiftorQueryStep.processSlicesMaterializedByFilters` synthesizes slices for coordinates explicitly pinned in the user filter (`Equals` / `In`) that are absent from the table, emitting `null` for them. A passthrough `Combinator(COALESCE)` would not — those coordinates would be silently absent from the output. Concretely, `groupBy=country, filter=country IN (FR, US, DE)` over a table holding only FR and US rows: Shiftor emits a DE row with `null`, a Combinator does not. That difference is observable whenever the table has gaps on user-pinned coordinates.

This synthesis is currently Shiftor-specific, but is really a cross-cutting concern: the same "should we emit a row at a user-pinned coordinate that's missing from the table" decision applies to Combinator, Filtrator and Aggregator paths. The cleanest fix is to lift it to a single engine-level post-processing pass shared across measure types; once Shiftor no longer owns the behaviour, a `ShiftorToCombinatorOptimizer` becomes safe and can be wired in alongside the Filtrator/Unfiltrator ones. Tracked as a TODO in `QueryStepsDagBuilder` next to the default-composite declaration.

Swap the default via `QueryStepsDagBuilder.withOptimizer(...)` — used by tests that need to assert on the un-optimised shape (e.g. error-path tests that exercise a deep measure chain).

After the optimizer runs, the builder verifies three invariants: every user-requested root is still in the graph, every pre-cached step is still in the graph, and every leaf captured before optimization (the steps that become `TableQueryStep`s on the table side) is still present. A buggy custom optimizer surfaces with a clear error here, not as a "missing cuboid" deep in the engine.

### Combinator-subgraph folding

A node is foldable iff its measure is a `Combinator` (any number of underlyings), it has exactly one incoming edge (single consumer), at least one outgoing edge, it is not user-requested, and it is not pre-cached. A connected subgraph of `n >= minChainLength` foldable nodes is replaced by a single fused step whose combination evaluates the captured subgraph in one per-cell pass.

The fused step's underlyings are the *distinct* boundary leaves of the subgraph (a non-foldable node reachable from one or more foldable internals). When the same boundary leaf is referenced from multiple foldable internals — e.g. `sum(branch_a, branch_b)` where both branches end at the same aggregator — it is registered once as an underlying, and the `ComposedCombinationPlan` routes both references through the same slot; the engine materialises that leaf's cuboid exactly once.

Linear chains are a degenerate case (every internal has exactly one underlying). Tree shapes (`Combinator.sum(chain_a, chain_b)`) and shared-leaf DAGs are handled by the same pass.

The fused step uses the topmost node's filter / groupBy / customMarker; subgraphs with heterogeneous values on those dimensions are skipped (logged at DEBUG). The fused combination is a `ComposedCombination` resolving each constituent through the operator factory, so any combination type the project knows about composes transparently.

### Why `ICombination` doesn't need to change

The composition layer is `ComposedCombination` + `ComposedCombinationPlan`. The per-stage primitive — `ICombination.combine(slice, slicedRecord, receiver)` — already expresses exactly what a tree node needs: the `slicedRecord` represents that node's K immediate children's values, the `receiver` is where the node's output goes. There is no information a tree node lacks that an API change would provide; the tree shape, scratch slot allocation, and adapter pooling are entirely composition-layer concerns. The current API is the right primitive.

See `TestFoldCombinatorSubgraphsOptimizer` for the optimizer contract, `TestComposedCombination` for chain / tree / shared-leaf plan evaluation, `TestFiltratorToCombinatorOptimizer` and `TestUnfiltratorToCombinatorOptimizer` for the per-measure rewrites, and `TestDagCubeQuery_LongChainPruning` for an end-to-end benchmark (~3.5× faster end-to-end with folding on, 10.3 s → 3.0 s).
