This document is intended for Adhoc developers and users.

It covers the core query engine and shared libraries. Pivotable-specific entries (the
SPA, the Pivotable backend, the dev stack, login flow, asset loading) live in
[pivotable/CHANGES.MD](pivotable/CHANGES.MD).

We adhere to the [keepachangelog](https://keepachangelog.com/en/1.0.0/) format


## [0.1.0]

Bump to 0.1.0 to mark the move to JDK 25 (still 0.x — Adhoc is not yet API-stable).

### Added
- Pluggable DAG optimizer (`eu.solven.adhoc.engine.optimizer`) — `IQueryStepsDagOptimizer` interface with `Noop`, `FoldCombinatorSubgraphsOptimizer` (chains and trees, shared boundary leaves deduplicated), `PartitionorToCombinatorOptimizer` (degenerate Partitionor → Combinator), `FiltratorToCombinatorOptimizer` and `UnfiltratorToCombinatorOptimizer` (redundant filter / no-op editor → COALESCE passthrough), and `Composite`. Default composite chains the rewrites then folds; swap via `QueryStepsDagBuilder.withOptimizer(...)`. Post-optimization sanity check verifies roots / cache / leaves survived. Long-chain benchmark ~3.5× faster (10.3s → 3.0s). New `ComposedCombination` evaluates a slot-based `ComposedCombinationPlan` on the primitive `(slice, slicedRecord, receiver)` path with a per-thread pool — no per-stage allocations. Registered under the short key `ComposedCombination.KEY = "COMPOSED"` so EXPLAIN renders `Combinator[COMPOSED]`. Fused-step naming is pluggable via `IComposedNameStrategy`; the default preserves each internal's `name[combinationKey]` (e.g. `ratio_postcheck[DIVIDE] ∘ whole[COALESCE]`). See `docs/optimization.md` for the design and trade-off rationale.
- `MultitypeArray.add()` is now a specialized override of the `IMultitypeArray` default; it routes each `IValueReceiver` callback to the pure-append entry point of the underlying `Chunked*List`, hitting the new append-cache directly. Callers with append-at-end intent (e.g. `MultitypeNavigableColumn.onAppendLast`) benefit automatically — `TestDagCubeQuery_LongChainPruning` measured ~12 % faster end-to-end.
- `ChunkedList<E>`, `ChunkedLongList` and `ChunkedDoubleList` gained an append-cache (`appendChunk` + `appendChunkBase` fields) that short-circuits the per-cell chunk arithmetic on consecutive appends. Trade-off: +8 bytes per instance.
- New `LambdaReceiverCombination` (in `eu.solven.adhoc.measure.lambda`): an alternative to `LambdaCombination` that exposes the primitive-friendly `ICombination.combine(ISliceWithStep, ISlicedRecord, IValueReceiver)` entry point. The lambda receives the `ISlicedRecord` and the output `IValueReceiver` directly, so a combination can read its inputs via `slicedRecord.read(i, receiver)` and emit via `receiver.onLong` / `.onDouble` without going through the boxed `List<?>` path. Useful when per-cell allocation/boxing cost matters.
- Pivotable Preferences modal: new "Enable Vue devtools" toggle. Persisted as `localStorage["adhoc.vueDevtools"]`; also activatable per-tab via the `?devtools` URL flag. The bootstrap in `index.html` reads the flag synchronously and sets `window.__VUE_PROD_DEVTOOLS__` before Vue's ESM module evaluates, so the Vue 3 devtools hook is exposed even on the minified build.
- Pivotable SPA: bumped vue-router 4.6.3 → 5.0.7 (which absorbs `unplugin-vue-router` into core; the SPA only uses the runtime `dist/vue-router.esm-browser.js`, no file-based routing) and vue 3.5.32 → 3.5.34 (vue-router 5.0.7's peer minimum). The vue-router 5 pom drags in unplugin build-time transitives that aren't all published as webjars; a wildcard `<exclusion>` blocks Maven from chasing them while the ESM browser bundle remains fully self-contained.
- Plan fragments: the live plan now also shows the table-engine secondary DAG (one node per merged `TableQueryV4`) and, for `JooqTableWrapper`-backed tables, an inlined-SQL leaf under each `TABLE_QUERY`. New write-only `IQueryPlanRegistry.publishFragment(queryId, anchor, subtree)` hook routes per-query observations into the matching `LiveQueryPlanSource`; idempotent within `(anchor, subtree.subject)`. New `IPlanFragmentSink` + `PlanFragmentScope` (JDK 25 `ScopedValue`) let any engine extension publish sub-nodes without learning the `(registry, queryId)` pair.
- Pivotable SPA: top-level `<BackendStatusBanner>` surfaces backend outages (metadata probe 5xx / network error) as a red alert with a "Retry now" button and a countdown to the next auto-retry (capped exponential backoff: 2s, 5s, 10s, 30s, 60s). State exposed on the adhoc pinia store as `backendStatus`.
- New `GET /api/v1/cubes/queries/{queryUuid}/plan/{summary,snapshot,children}` endpoints (webmvc + webflux). `queryUuid` is the UUID returned by `POST /cubes/query/asynchronous`; the engine adopts it as its own `AdhocQueryId.queryId` (see `SubmittedQueryIdScope`). Response contract: 200 when the plan is registered, 204+`Retry-After: 1` while still queueing, 204 for any terminal "no plan to serve" state, 404 only when the endpoint itself is missing — see [`docs/api.md`](docs/api.md). Default registry is `BoundedQueryPlanRegistry` (cap = 200 000 nodes).
- New `SubmittedQueryIdScope` (JDK 25 `ScopedValue`): thread-scoped override for `AdhocQueryId.queryId`. Lets a frontend pre-generate a tracking UUID and bind it around the submission call; without a binding the existing random-UUID default applies. Mirrors `CustomMarkerScope` / `QueryOptionsScope`.
- New SPA component `adhoc-query-plan-live.js`: badge polling `/plan/summary` at 2 Hz, showing state + done/total counts + elapsed/wait time + last-finished-step label. Honours the four-state response contract; stops on terminal state.
- New `GET /api/v1/cubes/queries/{queryUuid}/plan/children` endpoint: returns one `QueryPlanSummary` per child plan whose `parentQueryId` matches the UUID. Enables UI navigation over a composite query's fan-out.
- Removed the deprecated push-side plan-registry path: `StaticPlanSource`, `QueryPlanRegistryUpdater`, the `events/` package, and `IQueryPlanRegistry.register(QueryPlan)`. Callers use `registerSource(IPlanSource)` — `LiveQueryPlanSource` is now the single path.
- Pivotable SPA: TypeScript-via-JSDoc gate. Every `.js` source, Vitest spec, and Playwright spec carries `// @ts-check` and passes `tsc --noEmit`. Third-party libs (vue-router, mermaid, bootstrap, slickgrid, sortablejs, lodash) typed against their real `.d.ts`; pinia stays under an `any`-shaped ambient declaration. CI runs `npm run tsc_check` before vitest. Zero runtime cost.
- `RoutingMeasure` (`engine/recipes`): `ICombinator`-style measure dispatching each `CubeQueryStep` to one of several underlying measures via a `Function<CubeQueryStep, String>`. Callers must ensure queries do not straddle the routing boundary. Not Jackson-serializable — register programmatically.

### Changed
- `CubeQueryEngine` now prunes intermediate cuboids during `walkUpDag`. A per-step "remaining consumers" counter is initialised from the multigraph's incoming-edge count; once a step's counter hits zero and it is not in `queryStepsDag.getExplicits()` (the user-requested measures), its cuboid is removed from the in-flight `queryStepToValues` map so the GC can reclaim it. Cache pushes were moved from a post-walk `registerResultsToCache` sweep to eager pushes at the point each cuboid is freshly computed (in `executeTableQueries`' result loop and at the end of `onQueryStep`), so the cache decides its own retention policy before pruning happens. The `onQueryStep` signature gains a `ConcurrentMap<CubeQueryStep, AtomicInteger> remainingConsumers` parameter, and the obsolete `registerResultsToCache` method is removed. Without this pruning, a long chain of same-cardinality combinators accumulates every intermediate cuboid in heap and OOMs on large groupBy cardinalities (pinned by `TestDagCubeQuery_LongChainPruning`: a 10 000 × 10 000 +1 chain runs in ~40 s under the default 512 MB heap with pruning, vs. an `OutOfMemoryError` after ~30 s without it).
- `JooqTableQueryFactory.prepareSliceQuery(TableQueryV4)` now emits a SQL UNION ALL across `TableQueryV4#streamV3()` branches when `isPerfectV3()` is false, instead of inflating to a covering single GROUPING-SETS query via the deleted `TableQueryV4.asCoveringV3()`. The DB only computes the `(groupBy, aggregator)` pairs each branch actually requires. When `isPerfectV3()` is true, the path routes through the new `TableQueryV4.toV3()` helper (perfect-match-required, throws otherwise) — same single GROUPING-SET SQL as before. DRILLTHROUGH (`ColumnsManager.openStreamInternal`) also uses `toV3()` (single-groupBy guarantees perfect match).
- `CompositeCubesTableWrapper#getCoordinates(Map, int)`: override now fans out one bulk call per sub-cube (with the columns it actually carries) instead of falling back to `ITableWrapper`'s default per-column loop. Preserves the `JooqTableWrapper` bulk-SQL optimization end-to-end through a composite — a Pivotable navbar search across N columns now issues one SQL round-trip per Jooq-backed sub-cube rather than N. The `optCubeSlicer` virtual column is synthesised locally; merged samples union coordinates (truncated to `limit`) and sum estimated cardinalities.
- `CubeQueryEngine` no longer owns the plan registry — `StandardQueryPreparator` does. The preparator carries `queryPlanRegistry` (default `NoopQueryPlanRegistry.INSTANCE`) and writes it into the `QueryPod`; the engine no longer rebuilds the pod. `AdhocSchema` threads it through `makeQueryPreparator()`. Tests overriding `engine()` to wire a registry now override `queryPreparator()`.
- `BoundedQueryPlanRegistry` is lock-free: dropped `mutationLock` and every `synchronized` block. `locked` is a `ConcurrentHashMap`; `sources` is a Guava `Cache` with `maximumWeight` + node-count weigher (automatic LRU+weight eviction); `totalNodeCount` is an `AtomicLong` tracked via the cache's removal listener. Trade-off: an in-flight plan can be evicted under pure weight pressure (mitigated by UI pollers keeping it warm; callers can `lock(id)` for hard guarantees).
- Plan API is now graph-shaped (`/cubes/queries/{uuid}/plan/snapshot`): `QueryPlan` returns `{ rootId, nodes, edges: [{parentId, childId}] }` instead of a `root` tree. Each logical step appears exactly once in `nodes` — the projector's DAG memoization survives JSON round-trip. `QueryPlanNode` gains a server-assigned `id: String`; new `QueryPlanEdge` value type. SPA `planToMermaid` and `collectSqlLeaves` rewritten as flat iterations over `nodes` / `edges`.

### Added
- `QueryPlan` always carries a top-level `CUBE_QUERY` node with the N root `CubeQueryStep`s as children, so the legend is the same regardless of root count. The submitted query's structured detail breakdown travels through a new `LiveQueryPlanSource#cubeQueryDetails`. New `NodeOperator.CUBE_QUERY` enum value with a distinct subroutine `[[…]]` shape on the SPA Mermaid renderer.
- `QueryPlanNode#details` is now populated for every `CUBE_STEP` via a polymorphic `QueryPlanProjector.toDetails(Object)`. Iteration order is measure, filter, groupBy, customMarker, options; optional properties are omitted (matchAll filter, grand-total groupBy, null customMarker, empty options). SPA renders one `<br/>key=value` line per non-`sql` entry.
- Pivotable plan Mermaid modal: "Top-down ↔ Left-to-right" toggle re-renders the cached snapshot locally. LR is often more readable for wide plans. Modal-local — not persisted across opens.
- Pivotable plan Mermaid modal: per-step timings and rows render as colour-coded pill chips on each node (amber stopwatch for elapsed time, blue stack for rowsOut, purple arrow for rowsIn). Source data is `QueryPlanNode.stats` populated from `DagExplainerForPerfs`. PENDING nodes render unchanged.
- Pivotable plan Mermaid modal: state legend rendered below the diagram (Done = green, Running = blue, Pending/Skipped = grey, Failed = red). Swatches share `STATE_PALETTE` with the Mermaid `classDef` lines so the two cannot drift.

### Added
- Pivotable plan Mermaid: nodes grouped under engine-stage subgraphs ("Cube query steps", "Table query steps", "Table queries"). The CUBE_QUERY root and uncategorised "other" nodes stay at the top level. Empty subgraphs are omitted.

### Fixed
- Pivotable plan Mermaid: deep-equal subjects collapse to a single Mermaid node. Previously the SPA dedup'd on `node.subject` by reference, so N Jackson copies of the same Java step produced N Mermaid nodes. New `nodeDedupKey(node)` uses a stable string (`details.sql` for SQL leaves, `JSON.stringify(subject)` otherwise).
- Pivotable plan Mermaid: SQL leaf now reaches the plan tree even when the cube wires a column aliaser (e.g. `WorldCupPlayersSchema`). `ColumnsManager.openStreamInternal` publishes a bridge fragment from the original V4 to the transcoded V4 so the SQL leaf is reachable via the existing anchor. Skipped when no aliaser is configured.
- `QueryPlanProjector`: cycle guard against fragments whose root subject equals their own anchor (previously hanged `executeAsync` on the `publishInducedFragments` path). Re-entries are skipped with an SLF4J warn pointing the publisher at the unique-subject pattern.
- `TableQueryEngine` now publishes a plan fragment for every induced `TableQueryStep` (steps derived from an inducer's results via roll-up). Previously only inducers were published, so induced steps appeared as childless leaves disconnected from the `TABLE_QUERY` that served them. The fragment subject is a fresh marker to avoid the Jackson trap where `TableQueryStep#getTransverseCache()` throws for optimizer-split steps.
- Pivotable Formatting Options: flipping `primaryHeatmap` / `secondaryHeatmap` now repaints the grid immediately via `invalidateAllRows()` + `render()` instead of waiting for the next Submit.
- Pivotable full-screen-grid mode: hiding the wizard no longer surfaces a Refresh button stuck on "Refreshing…". Root cause: `AdhocGridControls` was injecting executor state from a sibling, falling back to truthy plain-object defaults. Fix: shared executor state lifted into a reactive `executorBus` bag owned by `AdhocQuery` and provided to the grid subtree.
- Pivotable scroll-mode column widths: switching from `fit` to `scroll` no longer leaves non-frozen columns narrower than their content. Header DOM was being located via `:nth-child`, which crossed SlickGrid's frozen/non-frozen panes; now resolved via the column id stamped on the header cell.
- Pivotable plan endpoints were unreachable in webmvc — `PivotablePlanController` was missing from `PivotableWebmvcSpringConfig`'s `@Import` block.
- Pivotable LiveView badge no longer reports "Queued" mid-flight: `LiveQueryPlanSource.markExecutionStarted` now flips `planState` from PENDING → RUNNING (previously it transitioned only to DONE/FAILED on completion).
- Pivotable SPA login chip ("You need to login") now opens the login modal in-place via `userStore.needsToLogin = true`, instead of navigating to `/html/login` (which caused a full document reload).
- Pivotable `/html/login` BASIC choice no longer triggers a full document reload: the route now renders `LoginProviders` with `:modal="true"` so the BASIC form is inlined rather than reached via an `<a href>` to `/html/login/basic`.
- Pivotable SPA plan-polling now uses `userStore.authenticatedFetch` (was plain `fetch`). 401 surfaces a "Log in to see the plan" hint, opens the login modal, and stops the poll loop. Same fix on the Mermaid detail modal.
- Pivotable: `pivotable-server` ships IDE-autocomplete metadata for the env-read `adhoc.pivotable.login.oauth2.*` keys via `additional-spring-configuration-metadata.json` and `spring-boot-configuration-processor`.
- Pivotable OAuth2 login-button override: a downstream `adhoc.pivotable.login.oauth2.enabled: false` (or per-provider) now disables the buttons even with `pivotable-demo_external_oauth2` active. The demo profile yaml no longer carries `enabled: true` lines that overrode user yaml.
- Pivotable: re-login on an existing query view no longer wipes the in-memory queryModel while the URL still carries the encoded model. Root cause: the model→URL watcher uses `history.pushState`, which bypasses vue-router, so `currentRoute.value.hash` was stale on remount. New `queryHelper.readUrlHash` reads `window.location.hash` directly.
- Pivotable grid timings bar no longer grows indefinitely after a failed query: new `finalizeActiveStages(timing, now)` walks every `<stage>_startedAt` marker, sets the stage to the time-until-failure and removes the marker. The `finally` block also clears every loading flag (was only `sending` and `downloading`).
- Pivotable SPA: dead-code references uncovered while enabling strict pinia types — `store.contests` / `loadSchemas` / `nbCubeFetching` / `players[playerId]` etc. Marked `@ts-nocheck`/`@ts-ignore` with TODOs; one runtime-dead `latestQueryIds >= 1` (array compared to number) rewritten to `.length >= 1`.
- Pivotable chat: `PivotableChatConfiguration` is always registered regardless of API-key presence. `GET /chat/enabled` returns 200 with `{enabled, reason?, retryAfterSeconds?}`; `POST /chat` returns 503 with the same body when unavailable. New `adhoc.pivotable.chat.enabled` toggle. Replaces the previous 404 when no API key was set.
- Pivotable grid auto-fit (dblclick on a column-header resize handle): footer row content is now preserved. The previous `grid.updateColumnsInternal()` call emptied the footer cells without re-invoking `gridHelper.updateFooters`; the new path applies widths via `applyColumnHeaderWidths` + `applyColumnWidths` + `resizeCanvas` without touching the footer DOM.
- Pivotable grid phantom column (scroll-mode horizontal-scroll headroom): the resize handle is now hidden via `.slick-header-phantom .slick-resizable-handle { display: none }`. The phantom column exists only to provide scroll headroom and is not meant to be resizable; the visible handle was confusing.
- Pivotable chat: `PivotableChatConfiguration` was never `@Import`-ed by `PivotableWebfluxSpringConfig`, so `/cubes/chat` had no route handler. Imported in both `PivotableWebfluxSpringConfig` and the new `PivotableWebmvcSpringConfig`.
- `CompositeCubesTableWrapper`: `EmptyAggregation` aggregators are forwarded to every sub-cube unconditionally (previously failed with `Measures [empty] are not known by any subCube` when no sub-cube exposed `empty`). An empty aggregator only materializes slices, so any sub-cube can serve it.
- `JooqTableQueryFactory.selectedRowsFields` (DRILLTHROUGH path): `COUNT(*)` now renders as `CASE WHEN <filter> THEN 1 END AS <alias>` instead of the literal `*`, which jOOQ expanded to every column and bound aliases to wrong values. SLICES path already special-cased this; ROWS path now matches.

### Changed
- Pivotable grid: heatmaps split into two independent toggles (both off by default). `primaryHeatmap` is the column-wide green→red cell background; `secondaryHeatmap` is the per-parent-group in-cell bar. Fresh load reads as a plain spreadsheet.
- Pivotable SPA: cross-layer library-version alignment is enforced. New `lib-version-alignment.spec.js` reads `pom.xml`, every importmap, `vite.config.js`'s `IMPORTMAP_ALIASES`, and `package.json` and fails CI on a partial bump. `renovate.json` adds per-library `groupName`s so future bumps land as a single PR.
- Pivotable SPA: bumped `lodash-es` WebJar from `4.17.21` to `4.18.1` across pom, importmaps, and vite.
- Pivotable SPA: bumped `mermaid` from `11.6.0` to `11.14.0` (latest WebJar; 11.15.0 is npm-only). No source-level API change.
- Pivotable grid auto-fit: cell width is now measured via Canvas2D `measureText` on the formatter output, not `cell.scrollWidth` (which couldn't shrink an already-too-wide column). Header chrome reservation grew from 14 to 42 px to cover the on-hover 3-dot menu icon.
- Pivotable grid column-resize handle: now symmetric around the column boundary. `.slick-header-column { overflow: hidden }` was clipping the right half of the SlickGrid Alpine handle; lifting it restores the right-side hit area. Column-name truncation still works via `.slick-column-name`'s own ellipsis.
- Pivotable grid: empty cells now render explicit greyed-italic placeholders — `null`/missing → `NULL`, `""` → `empty`, grand-total `*` → `Total`. Applies to groupBy, measure, and percent formatters in every view. Wire payload unchanged.
- `QueryStepsDagBuilder.rootMeasureless`: when a `ColumnWithCalculatedCoordinates` declares names that collide with real coordinate values, the natural sub-query gets a `NOT IN (<calc-coord names>)` filter so the calc-coord becomes authoritative. The grand-total marker `*` is excluded (crashes typed columns on coercion).
- New `TestPivotableMcpServer_SseRoundTrip`: e2e test booting a WebFlux app with the `pivotable-mcp` profile and exercising the SSE transport, JSON-RPC framing, and Spring AI `@Tool` → MCP-protocol bridge via an in-process `McpSyncClient`.
- Pivotable SPA: "Copy MCP token" affordance under the logged-in account chip. Surfaces the current `access_token` as a clipboard-ready `Authorization: Bearer <jwt>` line for an MCP client's `.mcp.json`. Short-lived; long-lived API-token / OAuth 2.1 flow is on the ROADMAP.
- New `pivotable-mcp-webflux` / `pivotable-mcp-webmvc` starter modules: each bundles `pivotable-mcp` with the matching Spring AI server starter. `PivotableMcpConfiguration` is `@Profile("pivotable-mcp")`-gated and now actually imported by both server modules — activating the profile finally exposes `/mcp/sse`.
- New `docs/mcp-integration.md`: walkthrough of the MCP feature (exposed `@Tool` methods, Maven + Spring profile wiring, client examples, test coverage) and the Pivotable Chat assistant (`ADHOC_PIVOTABLE_CHAT_ANTHROPIC_API_KEY`, SSE event vocabulary, WebFlux/WebMVC split).
- Pivotable chat now works under both WebFlux and WebMVC. Framework-agnostic prompt assembly + Anthropic-SSE translation moved into `pivotable-server-core` (`ChatRequestPlanner` + `AnthropicSseTranslator`); new WebMVC `PivotableChatController` mirrors the WebFlux handler using JDK `HttpClient` + `SseEmitter`. Default model bumped to `claude-haiku-4-5`.
- New `CustomMarkerScope` and `QueryOptionsScope` (`eu.solven.adhoc.factories`): thread-scoped access to the executing query's `customMarker` and `Set<IQueryOption>` via JDK 25 `ScopedValue`. `CubeQueryEngine#execute` binds both around the whole call; extension points (e.g. `ICalculatedColumn#computeCoordinate`) read them via `*.current()` without API widening. Outside a binding, `current()` returns empty.

### Added
- New `CubeColumnsWrapper` (`engine/cube`): owns column resolution for `CubeWrapper` (table + calculated + generated columns with caller-facing aliases). Memoised, with `invalidateAll()` to pick up schema changes. `CubeWrapper` and `CompositeCubesTableWrapper` now implement `IHasCache`.

### Changed
- Aggregator restructuring: `adhoc` and `adhoc-table` are nested under a new `engine/` aggregator. `adhoc-table` → `engine/table/`; former `engine/adhoc/` → `engine/recipes/` (artifactId `adhoc` → `adhoc-recipes`, automatic-module-name `eu.solven.adhoc` → `eu.solven.adhoc.recipes`). Downstream consumers must update their `<artifactId>`.
- `@NullMarked` (JSpecify) rolled out across `adhoc-model`, `adhoc-cell`, `adhoc-encoding`, `adhoc-filters`, `adhoc-dataframe`, `adhoc-measures`, `adhoc-table` (engine/table), `adhoc-recipes` (engine/recipes — formerly `adhoc` / engine/adhoc). Every package declares `@NullMarked` via `package-info.java`; explicit `@Nullable` opt-outs for legitimately-nullable references. Modules pass NullAway clean. Notable contract changes (matching pre-existing `return null` paths and documented semantics):
  - Aggregation/combination contracts: `IAggregation#aggregate`, `ICombination#combine`, `IAggregationCarrier.IHasCarriers#wrap` accept/return `@Nullable Object`.
  - Matcher contracts: `IValueMatcher#match` and every implementation accept `@Nullable Object`.
  - Lookup contracts: `Map#get`-style methods (`ITabularRecord#getAggregate`, `Slice#getGroupBy`, `PerfectHashMap`, `MutablePerfectHashMap`, `AAggregatingColumns#getColumn`, `IInt2ObjectReader#read`, `IReadableColumn#readValue`, …) return `@Nullable`.
  - Cancellation/customMarker: `IIsCancellable#getCancellationDate`, `CubeQuery#customMarker`, `MeasurelessQuery#customMarker`, `IFilterEditor.FilterEditorContext#customMarker`, `IHasCustomMarker#getCustomMarker`, `ACustomMarkerCombination#combine`/`getDefault`, `ICustomMarkerTranscoder`, `ICustomMarkerCacheStrategy#restrictToCacheImpact` are `@Nullable Object`.
  - Transcoders: `IColumnValueTranscoder#transcodeValue`, `ICustomTypeManagerSimple#toTable`, `ICustomTypeManager#fromTable`, `CubeWrapperTypeTranscoder#toTable` accept/return `@Nullable Object`.
  - `IGroupBy#getColumn(String)` now throws `NoSuchElementException` for an unknown column (previously returned `null` silently — incompatible with `@NullMarked`).
  - Lombok `@Builder` cases across the modules carry `@SuppressWarnings("NullAway.Init")` on the inner builder class.
- NullAway 0.13.4 is wired into the `errorprone` profile (opt-in via `-Perrorprone`), running as an Error Prone plugin in `JSpecifyMode=true` with `OnlyNullMarked=true`. CI's `static-analysis` workflow fails on any unsafe deref/pass/return inside a marked package.
- `engine/table/pom.xml` jacoco thresholds lowered from 0.70/0.78 to 0.50/0.55 to reflect the v19 module split (most tests still live in `engine/recipes`).

## [0.0.19]

### Added
- `StandardQueryOptions.DRILLTHROUGH`: production-ready. All `TableQueryV4`s of the query DAG are merged into a single covering query (new `TableQueryV4Merger`); a dedicated `TableQueryEngine#executeDrillthrough` streams every table row into `ListMapEntryBasedTabularViewDrillThrough`, bypassing per-slice aggregation. Pivotable: the option is surfaced in the wizard and a "DrillThrough this cell" button on the per-cell modal pins the row's coordinates as filters.

### Changed
- `ForestAsMermaidDag` (`experimental` module): Mermaid-flowchart counterpart of `ForestAsGraphvizDag`. Renders an `IMeasureForest` (or `MeasureForests` with one `subgraph` per forest) as `flowchart LR` text using the same per-class shape/color conventions. Supports a `highlightedMeasures` builder option.
- `ThrowingCombination`: promoted from test scope to main scope so downstream projects can register it to exercise error-management paths. Behaviour unchanged.

### Fixed
- `TableQueryEngine#suppressGeneratedColumns`: a groupBy on a column that is both generator-produced and `withStar` no longer collapses both branches to the same `grandTotal` query (which crashed `appendSlice` on duplicate keys). Suppression now skips any groupBy column whose `IAdhocColumn` is an `ICalculatedColumn`.
- `ForestAsGraphvizDag.highlightedMeasures`: new builder option; matching nodes get a thick red double-perimeter border on top of their normal style.
- `IHasExecutorAndSliceFactory` + `PodExecutors` + `IScopeBinder`: new utilities to centralise scope-binding around dispatched tasks. `ISliceFactory`'s `callWithScope` / `runWithScope` / `getWithScope` are deprecated in favour of `ColumnSliceFactory` implementing `IScopeBinder#bindScope` directly.
- `PerfectHashMap` / `PerfectHashKeyset`: immutable `Map<String, V>` backed by a reusable `PerfectHashKeyset` and an `ImmutableList<V>` of values. The keyset is computed once and shared across many maps; each map only pays for its values list. Public `ABSENT` sentinel for "not in this map". Intended for callers like `TabularRecordBuilder#aggregates`.
- `ScopedValueAppendableTable`: JDK-25 `ScopedValue`-backed analogue of `ThreadLocalAppendableTable`. Each caller thread establishes its own page-map via `runInScope(Runnable)` / `callInScope(Callable)`; `nextRow` outside a scope throws `NoSuchElementException`. Intended for faster per-request access on Virtual Threads versus `ThreadLocal.get()`.
- `ScopedValueAppendableTableFactory`: opt-in alternative to `ThreadLocalAppendableTableFactory`. Wired via `ISliceFactory.callWithScope` / `runWithScope` / `getWithScope`; dispatch sites in `CubeQueryEngine`, `PartitionorQueryStep`, `TabularRecordStreamReducer` establish a scope per virtual-thread task. ThreadLocal remains the default.
- `StandardQueryOptions.PARTITIONED`: new query option that splits the output column into N hash partitions (one per available core) during aggregation. Each partition is an independent `PartitionedMergeableColumn`, reducing per-partition map size and enabling future parallel reduction across cores.
- `IConsumingStream.forEachPartitioned`: partition-aware terminal operation that routes elements to dedicated per-partition consumer threads, eliminating write contention. Used by `TabularRecordStreamReducer` when the `PARTITIONED` option is active.
- `LastLookupCache1`: new length-1 specialization of `LastLookupCache` for hot-path callers that only need a single reference key. Avoids the `Object[]` varargs allocation and list wrapping on the slow path.
- `LastLookupCache1Volatile`: alternative length-1 cache using a single `volatile` shared slot instead of a `ThreadLocal`. Eliminates the `ThreadLocal.get()` map lookup at the cost of cross-thread contention on the slot. A JMH benchmark in `jmh/.../BenchmarkLastLookupCache` compares both variants against a plain `ConcurrentHashMap` baseline.
- `LastLookupCache1Scoped`: JDK-25 `ScopedValue`-backed length-1 cache. `ScopedValue.get()` replaces `ThreadLocal.get()` on the fast path; silent-miss outside a scope so the cache remains callable from static contexts.
- `SequencedSetUnsafe.internKeyset(Collection)`: new static utility that interns a column keyset into a `SequencedSetLikeList`, backed by a shared two-layer cache.
- `MultitypeNavigableIntColumn`: int-specialized companion to `MultitypeNavigableColumn`. Stores ascending int keys in a primitive `IntArrayList`, exposing `append(int)` / `onValue(int)` / `appendIfOptimal(int)` without `Integer` boxing.
- `IJooqTableSupplier` + `JooqTableWrapperParameters.tableSupplier`: opt-in hook to substitute a per-query `TableLike` in place of the constant `JooqTableWrapperParameters.table`. Schema introspection (`getColumns`, `getResultForFields`) still uses the constant.
- `PrunedJoinsJooqTableSupplierBuilder` (renamed from `PrunedJoinsJooqSnowflakeSchemaBuilder`): builds an `IJooqTableSupplier` that, per `TableQueryV4`, computes the minimal set of joins needed to serve the query's columns and folds them onto the base table. Closes parent links for snowflake transitivity; forces in non-prunable joins (those registered without a column contract). Bounded LRU (128 entries) caches the per-`referencedColumns` decision. Motivation: DuckDB pays a per-join cost in `FROM` even when the join contributes no `SELECT` column.
- `JooqTableSupplierBuilder.build()` / `IJooqTableSupplier.getSchemaTable()`: the supplier now exposes both `tableFor(TableQueryV4)` and `getSchemaTable()`, replacing the legacy two-step `.table(schema.getSnowflakeTable()).tableSupplier(supplier)` with a single `.tableSupplier(schema.build())`.

### Fixed
- `CompositeCubesTableWrapper`: a filter or groupBy on a column declared `generated` / `calculated` by at least one sub-cube is now auto-suppressed for sub-cubes that do NOT declare the column (previously translated to `matchNone` / cube-name fallback coordinate). Fixes composite-cube counts when `count(*)` is queried alongside a scenario hierarchy known only to some sub-cubes.
- `FilterHelpers.filterComparator()`: now correctly returns 0 for semantically equivalent filters with different operand order (e.g., `AND(a,b)` vs `AND(b,a)`). Type ranking: AND > OR > NOT > COLUMN.
- `IConsumingStream.iterator(ExecutorService)`: calls `endOfData()` on the poison marker. Previously the iterator returned `null` without transitioning Guava's `AbstractIterator` to `DONE`, so any subsequent `hasNext()` blocked indefinitely.

### Changed
- `ColumnFilter` JSON/YAML: `nullIfAbsent` is omitted from serialization when the underlying `IValueMatcher` cannot match `null` (no observable effect). Deserialization defaults to `true` when absent.
- Build profiles `-Pfast` and `-Pnostyle`: now set `cyclonedx.skip=true` so the CycloneDX SBOM generation is skipped during fast recompiles and style-skipping test runs.
- `MeasureForestConcealer`: operand values inside every `ColumnFilter.valueMatcher` are now concealed to `v_<hash>` tokens (previously only column names were hashed). Operator keys (`aggregationKey`, `combinationKey`, `editorKey`, `decompositionKey`) are concealed to `k_<hash>` unless they belong to a per-kind whitelist. **API change**: `ConcealingDefinition.valueMapping` is now `Map<Object, String>` (was `Map<String, String>`) and gains a sibling `keyMapping`.
- `TabularRecordBuilder.aggregates`: switched from `ImmutableMap.Builder` to `PerfectHashKeyset.Builder`; the perfect-hash table is built once per `AggregatedRecordFields` and reused by every record. **Behavior change**: duplicate aggregate column names are rejected at first record build instead of per-record.
- `GroupByColumns.retainAll`: `RETAIN_CACHE` is now a per-instance `LastLookupCache1` keyed on `retainedColumns` alone (was a static length-2 cache keyed on `(this, retainedColumns)`). Drops the slow-path `Object[]` allocation and the fast-path per-element loop.
- `CubeWrapper.getColumns()`: aliases whose underlying column cannot be resolved on the cube are now discarded with a `WARN` log, instead of registered as dangling `Object.class` columns. Prevents schema pollution when a `ColumnsManager` is shared across cubes.
- `AMultitypeNavigableColumn`: new abstract parent factoring shared write/append/scan/stream/lookup logic out of `MultitypeNavigableColumn` and `MultitypeNavigableIntColumn`. Subclasses own only the key-storage hooks. The protected helper `merge(int)` was renamed `mergeAtIndex(int)` to avoid colliding with `IMultitypeMergeableColumn.merge(T)` on the int specialization.
- `AMultitypeHashColumn`: new abstract parent for the hash family — identical extraction to `AMultitypeNavigableColumn`. `MultitypeHashColumn` and `MultitypeHashIntColumn` shrink to fastutil map declarations and one-liner forwarders.
- `MultitypeNavigableIntMergeableColumn`: new int-specialized companion to `MultitypeNavigableMergeableColumn` (unboxed `merge(int)` fast path). Not yet wired into `AggregatingColumns.makePreColumn()` — slice-sortedness tracking would need to move out of `MultitypeNavigableElseHashMergeableColumn` first.
- `UndictionarizedColumn.column`: tightened from `IMultitypeColumnFastGet<Integer>` to `IMultitypeIntColumnFastGet` — hot-path `onValue(int)` lookups never pay `Integer.valueOf` allocation. Also implements `ICompactable`.
- `UndictionarizedColumn.stream(SORTED_SUB / SORTED_SUB_COMPLEMENT)`: switched from positional `limit`/`skip` to `sortedLeg`-predicate filtering. Positional splits were only correct when the inner column iterated in slice-sorted order — no longer guaranteed after the refactor below.
- `AggregatingColumnsDistinct.makePreColumn()`: instantiates `MultitypeNavigableIntColumn` directly (was `IColumnFactory.makeColumn`). Indices arrive in ascending order so the append-last fast path is always hit, and no `Integer` boxing happens in the hot paths. `aggregatorToAggregates` is now typed `Map<String, IMultitypeIntColumnFastGet>`.
- `AggregatingColumns.undictionarizeColumn`: copies the generic `IMultitypeColumnFastGet<Integer>` into a fresh `MultitypeNavigableIntColumn` before wrapping in `UndictionarizedColumn`. Already-int-specialized columns short-circuit.
- `IMultitypeIntColumnFastGet.purgeAggregationCarriers()`: covariant narrowing — return type is now `IMultitypeIntColumnFastGet` so callers skip an `instanceof` re-check.
- `AdhocUnsafe.adhocCommonPool`: replaced `ForkJoinPool`-based work-stealing pool with `Executors.newVirtualThreadPerTaskExecutor()`. Virtual Threads are blocking-friendly, eliminating the need for a separate I/O thread pool.
- `QueryPod`: removed `dbExecutorService` field — a single `executorService` (Virtual Thread executor for concurrent queries) now covers both DAG walking and table I/O.
- `StandardQueryPreparator`: removed `concurrentDBExecutorService`, `nonConcurrentDBExecutorService`, `getDBExecutorService`, and the `isDuckDB` check; all queries use the same `executorService`.
- `AdhocTableUnsafe.setDbParallelism`: now a no-op kept for source compatibility; DB concurrency is controlled by `AdhocDuckDBUnsafe.duckDBSemaphore` instead of a thread pool.
- `AdhocDuckDBUnsafe`: replaced `adhocDuckDBPool` (fixed-size thread pool) with `duckDBSemaphore` (`Semaphore(2)` by default). Limits concurrent DuckDB queries without blocking Virtual Threads.
- `ITableWrapper`: new `querySemaphore()` default method returning `null` (no limit). Implementations can return a `Semaphore` to cap concurrent queries.
- `JooqTableWrapperParameters`: new optional `querySemaphore` field. `JooqTableWrapper` implements `querySemaphore()` from this parameter and applies it lazily in `streamSlices()`.
- `DuckDBHelper.parametersBuilder(IDSLSupplier)`: new factory method returning a `JooqTableWrapperParameters` builder pre-wired with `AdhocDuckDBUnsafe.duckDBSemaphore`.
- `TableQueryEngineBootstrapped` → `TableQueryEngine`; `TableQueryEngine` → `TableQueryEngineFactory` (rename).
- `ICombination`: signature changed from returning an `IValueProvider` to accepting an `IValueReceiver` as input, enabling push-based value delivery and avoiding intermediate allocations.
- `LastLookupCache`: single generic parameter `<V>` (dropped the custom map-key type parameter); a required `keyLength` is now passed to the constructor; `slowComputeIfAbsent` takes only a `Supplier<V>` and uses `Arrays.asList(refKeys)` as the delegate map key.
- `ISliceFactory.internKeyset(Collection)`: removed from the interface — replaced by `SequencedSetUnsafe.internKeyset(Collection)` (static). Callers that previously called `sliceFactory.internKeyset(keys)` should now call `SequencedSetUnsafe.internKeyset(keys)`.

## [0.0.18]

### Fixed
- `AddSharedNodes`: fixed missing sub-sharing when a newly inserted shared node's induced children contain further sharing opportunities; new node is now processed eagerly (via recursive `addSharedNode`) before control returns to the caller. Also replaced the `TopologicalOrderIterator`+dirty-requeue loop with a single linear pass over a pre-snapshotted `ImmutableList` (JGraphT contract: iterator must not be used while the graph is mutated).
- `TableWrapperHelpers.closeAll`: when closing multiple `ITabularRecordStream`s, exceptions from one stream no longer prevent the others from being closed; subsequent exceptions are attached as suppressed causes on the first.
- `InduceByAdhoc.splitInducedDag`: fixed NPE when input steps have non-contiguous groupBy cardinalities (e.g. sizes 1 and 3 with no size-2 step) — iterating through missing cardinalities now yields an empty list instead of `null`.

### Changed
- `InduceByAdhocComplete`: when `StandardQueryOptions.CONCURRENT` is active, each measure×context group is processed in an isolated local DAG on the injected `ListeningExecutorService`, then merged before transitive reduction. Default executor stays sequential (`MoreExecutors.newDirectExecutorService()`).
- `InduceByAdhocComplete.splitInducedAsDag`: O(n²) pairwise scan replaced by three pre-computed indices (measure+context grouping, groupBy containment, filter strictness). `makeLeftoverFilter` is only invoked when both indices agree.

### Added
- `IFilterFactories`: new interface coupling `IFilterStripperFactory` + `IFilterOptimizerFactory` at configuration time, with a `makeQueryBundle()` default method that produces a per-query `IFilterQueryBundle` (contextualised stripper + cached optimizer). `IAdhocFactories` now extends `IFilterFactories`.
- `IFilterQueryBundle`: new interface grouping the query-scoped `IFilterStripperFactory` and `IFilterOptimizer` that are created once per query and shared across all sub-strategies (`ITableStepsSplitterFactory`, `IMergeInducersFactory`, `IAddSharedNodesFactory`). Inner factory methods now take a single `IFilterQueryBundle` parameter instead of the previous two-argument pair.
- `AdhocUnsafe.adhocDbPool`: dedicated thread pool (default 2 threads, unbounded queue) for external database queries (e.g. DuckDB), preventing I/O-bound DB calls from starving the CPU-bound common pool. Configurable via `-Dadhoc.dbParallelism=N`. `StandardQueryPreparator` exposes a `dbExecutorService` field (defaulting to `adhocDbPool`), enabling per-`CubeWrapper` configuration; `QueryPod` propagates it to `TableQueryEngineBootstrapped`, which submits concurrent `TableQueryV4` queries to it instead of using parallel streams on the CPU pool.
- `FilterOptimizer`: the AND optimizer now clusters operands by independent column sets before optimising. Operands whose column sets are fully disjoint are optimised independently and then AND-ed, enabling the distributive-law folding `(A&B)|(A&C) → A&(B|C)` to fire correctly. `FilterHelpers.clusterFilters` exposes the clustering logic.
- `ICubeQueryStep`: new interface grouping the contracts of `CubeQueryStep` (`IWhereGroupByQuery`, `IHasMeasure`, `IHasCustomMarker`, `IHasQueryOptions`, `IHasCache`); `CubeQueryStep` now implements it, enabling polymorphic handling of step types.
- `TableQueryStep`: strongly-typed record wrapping a `CubeQueryStep` whose measure is guaranteed to be an `Aggregator`; replaces the raw `(Aggregator) step.getMeasure()` cast in the table-query execution path. `StepAndFilteredAggregator.step` now carries a `TableQueryStep` instead of a plain `CubeQueryStep`.
- `FlatAndFilter`: compact `IAndFilter` backed by `ImmutableMap<String, IValueMatcher>` — operands are guaranteed flat per-column conditions with no recursive `ISliceFilter` children. `ISlice.asFilter()` now returns a `FlatAndFilter`, and `FilterHelpers.emitAndOperands` / `FilterHelpers.asMap` have dedicated fast-paths for it. `SliceFilterSerializer` serialises it as the canonical `"type":"and"` form.
- `scripts/check-coverage-sonar.pl`: offline Perl script that queries the SonarCloud API (no local build needed) and lists production files below a configurable coverage threshold (default 20 %). Run with `perl scripts/check-coverage-sonar.pl [--threshold N]`.
- `scripts/check-coverage-low.pl`: local Perl script that parses JaCoCo XML reports and reports the same threshold violations; runs automatically in CI after the test step.
- `CONVENTIONS.MD` Test Coverage section: documents the 80 % instruction-coverage target and the 20 % critical floor.
- `TpchSchema`: TPC-H example schema using DuckDB's built-in `tpch` extension (`CALL dbgen(sf=0.1)`); builds a snowflake view (`lineitem → orders → customer → nation → region`, `lineitem → part`, `lineitem → supplier`) with measures for `revenue` (expression aggregation), `quantity`, `avg_discount`, `order_count`, and `customer_count`. Accompanied by `TestTableQuery_DuckDb_Tpch`.
- `TableQueryV4`: new query model that explicitly maps each `IGroupBy` to the exact `FilteredAggregator`s it needs, eliminating the cartesian-product waste of `TableQueryV3`. `streamV3()` collapses groupBys with identical aggregator sets into a single GROUPING SET and emits the rest as independent V3s (UNION ALL). `TableQueryFactory.processSteps` now builds V4 internally.
- `MeasureForestConcealer`: utility that produces a "concealed" copy of a `MeasureForest` safe for export — measure names, column names, filter column names, and tags are replaced by deterministic hashes; options are preserved unchanged by default (extensible via `concealOptions()`). `concealWithDefinition()` also returns a `ConcealingDefinition` that a trusted party can pass to `restore()` to recover the original forest.
- `MeasureForestAnalyzer`: utility that analyses a `MeasureForest` and returns (1) `columnToSpecialValues` — a map from every referenced column to the set of equality/IN-membership values in filter conditions, and (2) `tagToColumns` — a `SetMultimap` from tags to the columns referenced by measures carrying that tag.

### Changed
- Upgraded SpringBoot to `4.1.0-M2`
- Upgraded DuckDB to `1.5.0.0`
- Upgraded Arrow to `19.0.0`
- Renamed `IAdhocSlice` into `ISlice`.
- Renamed `SliceAsMap` into `Slice`.

## [0.0.17]

### Added
- Arrow IPC streaming endpoint: `POST /api/v1/cubes/query` with `Accept: application/vnd.apache.arrow.stream` returns query results as a streaming Arrow IPC response. Backed by `TabularViewArrowSerializer` (in `adhoc-experimental`) discovered via `ServiceLoader`. Supports `Long`, `Integer`, `Double`, `Boolean`, `LocalDate`, and `String` column types.
- `MavenDependencyAsMermaid`: utility that scans `pom.xml` files across a multi-module Maven project and renders a Mermaid `flowchart TD` of inter-module dependencies; compile edges use solid arrows, test-scope edges use dashed arrows, and modules are grouped into `subgraph` blocks by their aggregator parent.
- `ColumnarTabularView`: columnar alternative to `ListBasedTabularView` — stores one `List<?>` per coordinate column and one per aggregate, enabling zero-transposition Arrow serialization.
- `DuckDBMemoryLogger`: daemon that periodically samples `duckdb_memory()` and logs results; self-terminates when the monitored connection closes.
- `TableQueryV3` enables `GROUPING SET` queries.
- `ITableQueryStepsSplitter` enables to inject a splitting policy into an `ITableQueryOptimizer`.
- `ITableStepsGrouper` is introduce to facilitate the customization of `TableQueryV3` granularity. It is the opportunity to have dedicate `TableQueryV3` on specific columns (e.g. very high cardinality ones).
- `ITableStepsSplitter` is introduce to facilitate the customization of `CubeQueryStep` which should be induced by Adhoc. It is the opportunity to have dedicate `TableQueryV3` on specific columns (e.g. very high cardinality ones).
- Introduced `AdhocCaseInsensitivityUnsafe`. Beware, CaseInsensitivity is not fully ready yet.
- Introduce `Shiftor.ShiftorBuilder.lambda(ILambdaFilterEditor)` which enables defining an `IFilterEditor` through a lambda.

### Changed
- `ISliceToValue` is renamed `ICuboid`.
- Introduce `adhoc-dataframe`, `adhoc-measures`, `adhoc-cell` maven modules.
- `ITableQueryOptimizer` implementations has been fully reworked given `TableQueryV3`
- `TableQueryOptimizer` is renamed `TableQueryFactory` as it has no more any optimizing logic.


## [0.0.16]

### Added
- Introduce `Combinator.CombinatorBuilder.lambda(ILambdaCombination)` which enables defining an `ICombination` through a lambda.
- Introduce `Aggregator.AggregatorBuilder.lambda(ILambdaAggregation)` which enables defining an `IAggregation` through a lambda.
- Introduce `adhoc-encoding` module, to remove core algorithms from Adhoc Query Engine.
- Prepare partitioning of table queries (e.g. `IQueryPartitionor`), to help concurrency in table own engine, and in Adhoc by received partitioned results.
- `DuckDBTableWrapper` enables Arrow-support for fetching query results.
- `AdhocUnsafe.getNullComparator()` for `Map` comparison. This behavior can be customized with `AdhocUnsafe.setNullComparator()`.
- Enable customization of JDBC `fetchSize` through `JooqTableWrapperParameters.statementFetchSize`. Default is `16*1024=16_384`.
- Add probe to warn if JDBC `autoCommit` is false.
- Introduce DuckDB as an option to reduce induced tableResults (see `IInducedEvaluator`).
- Introduce `ClickHouseTableWrapper` to enable Arrow connectivity.

### Changed
- Optimize `CompositeCubesTableWrapper.transcodeSliceToComposite` by computing the missing columns mask only once, and leveraging `IAdhocSlice.addColumns(Map<String, ?>)`.
- `adhoc-atoti` dependencies over ActivePivot/Atoti are not minimal (e.g. no more `activepivot-server-spring`).
- Move Checkstyle configuration so that a call to `mvn checkstyle:check` relies on the standard Adhoc configuration.
- `DSLSupplier` is renamed `IDSLSupplier`.
- `AdhocMapHelpers.immutableCoyOf(Map)` is moved to `ImmutableHelpers.copyOf(Map)`
- Introduced `testcontainers` to check Adhoc against various Databases (PostgreSQL, YugaByteDB, CLickHouse, MySQL, MariaDB, )

### Fixed
- `SequencedSetLikeList.toArray()` now returns a sequenced array. It fixes bugs around `ImmutableList.copyOf()`.
- `AbstractAdhocMap.compareTo` was not managing `NULL_HOLDER` properly.

## [0.0.15]

### Added
- Introduce `AsynchronousFreezingStrategy` which will freeze in a different thread and then update some reference.
- Introduce `IColumnFactory.makeColumnRandomInsertions(IAggregation, int)` which do not rely on sorting, useful when iteration order is random.

### Changed
- Prevent `QueryStepRecursiveAction` from executing multiple times shared tasks in a DAG.
- `AbstractAdhocMap` does not extend `AbstractMap` to ensure all default methods have optimal implementation.
- `DagCompletableExecutor` enable task sharing when evaluating a DAG of tasks.
- `IAppendableColumn.freeze()` turned to `IAppendableColumn.freeze(IFreezingStrategy)`
- `MultitypeNavigableColumn` propagates the capacity to values.

### Removed
- `ISliceFactory.newMapBuilder()` is removed. You need to have a clear view of the keyset.
- `IMultitypeColumnFastGet.set(T)` is removed. You can rely on `.append(T)` which has a clearer semantic.

## [0.0.14]

### Added
- Spring Actuator Health is enriched through `AdhocSchemaHealthIndicator`
- FSST String compression is a `IFreezingWithContext`. Relates with https://github.com/cwida/fsst
- Introduces `IAdhocMap.retainAll` to make an immutable copy with a subset of columns.

### Changed
- `ISliceFactory` actually enable compression, based on dictionarisation, or `long[]` or `int[]`, or FSST for `String`.

### Fixed
- `CubeQuery.CubeQueryBuilder.andFilter(String, Object)` now relies on `ColumnFilter.matchLax(String, Object)`.

## [0.0.13]

### Added
- Experimental support of [MongoDB](https://www.mongodb.com/docs/languages/java/)
- Introduce multiple `.executeAsync(...)` methods, as alternative to usual `.execute(...)` returning a `ListenableFuture`.
- Introduce `QueryPod.addCancellationListener(Runnable)`, enabling registration and cancellation of inner parts when cancelling the outer query.
- `IAdhocMap.retainAll(Set<String>)` enables faster creation of `IAdhocMap` over a subset of columns (e.g. inducing).

### Changed
- Refactored `ISliceFactory`, preparing the ability to rely on dictionarized and column-oriented storage.

### Removed

- `DSLSupplier.fromConnection(Supplier<Connection>)` has been removed.

### Fixed
- `FilterOptimizer` may skip `or vs and` simplification (e.g. `FilterBuilder.or("a==a1&(b==b1|c==c1)").optimize()`).
- `FilterOptimizer` may skip `or vs and` simplification (e.g. `FilterBuilder.or("a=in=(a1,a2)&b==b1|a=in=(a1,a2)&!(b!=b2&!(c==c1&d==d1&b==b3))").optimize()`).
- `FilterStripper` was not properly stripping `OR` in case one stripped operand was `matchAll`.

### Improved
- `TableQueryOptimizerSinglePerAggregator` now build a tree of intermediate CubeQuerySteps, to enable computing each filters a single time.
- `SimpleFilterEditor.shift` has been optimized.

## [0.0.12]

### Added
- Introduced `InternalQueryOptions.ONE_TABLE_QUERY_PER_AGGREGATOR` to force `TableQueryOptimizerSinglePerAggregator` from the options.
- Enable custom `ISliceFilter` in `FILTER` in `TableQueryV2`
- `AndFilter.copyOf` and `OrFilter.copyOf` enables very fast creation of `AndFilter` and `OrFilter` is all optimizations has to be skipped.
- Add `TableQueryEngine` information in the `EXPLAIN`. In particular, describe the `inducerToInduced` DAG similarly to `DagExplainer`.

### Changed
- Renamed `ITabularGroupByRecord.groupByKeySet()` into `ITabularGroupByRecord.columnsKeySet()`.

### Removed
- Removed `IAdhocSlice.getRawSliced(String)`. Prefer `IAdhocSlice.getGroupBy(String)`.
- Removed `Combinator implements IMayHaveColumnGenerator` as only `IDecomposition` seems a reasonable option. 
- Removed `GeneratedColumnsPreparator` as `IMayHaveColumnGenerator` are automatically added by `StandardQueryPreparator`

### Fixed
- Fix bug in SQL condition with NOT given NULL. (Typically `NOT(c = 'v')` is turned into `NOT(c IS NOT NULL AND c = 'v')`).
- Memory-leak in `CachingOperatorsFactory` due to `DispatchorQueryStep` adding values as option, for `CumulatingDecomposition`.
- Improve compaction in `IMultitypeMergeableGrid.closeColumn`

### Improved
- `FilterHelpers.getFilteredColumns` is faster by relying on `Stream.mapMulti`
- `FilterOptimizer.optimizeAndOfOr` skips operands which are alone on given column, even if they hold an `OR`.
- `TableQueryOptimizer.splitInducedAsDag` may register an additional CubeQueryStep in order to apply the `FILTER` clause only once for a step of induced steps.

## [0.0.11]

### Added
- Introduced `ISliceReader` to help reading coordinate given a `ISliceWithStep`. Usage is `slice.sliceReader().getValueMatcher(c)` instead of `FilterHelpers.getValueMatcher(slice.asFilter(), c)`.
- Introduced `FilterBuilder` to help building `ISliceFilter` with an explicit way to skip or not optimizations. It replaces `AndFilter.and` and `OrFilter.or` .
- Introduced `AdhocUnsafe.sliceFilterOptimizer` and `IFilterOptimizerHelpers` to enable customization of `ISliceFilter` optimizer.
- Enabled `ISliceFilter` over an `ICalculatedColumn`. It works by post-filtering after adding in groupBy underlying columns.
- Introduced `FilterEquivalencyHelpers` to help evaluating equivalent `ISliceFilter`. It is useful to workaround issues related to `ISliceFilter` optimizations, which leads to equivalent boolean expression having different representations.
- Introduced `FilterOptimizerHelpersWithCache.java`, to enable caching of intermediate `ISliceFilter`.
- Introduced `JooqTableWrapper.fieldsCache` to prevent fetching all columns on each query (especially on `CompositeCube` scenario).
- Introduced `IFilterStripper` to enable customization and cache-friendly behavior around `stripWhereFromFilter` and `isStricterThan` methods.

### Changed
- `ITableQueryEngine` is now a field of `CubeQueryEngine` (instead of `.makeTableQueryEngine` factory method).
- `OrFilter.or` now relies on `AndFilter.and` to apply the same set of optimizations.
- `CompositeCubesTableWrapper` now rejects queries referring to unknown columns (in `groupBy` or `filter`).
- `CompositeCubesTableWrapper` now apply filters on unknown column in subCube as if the whole subCube has the `onMissingColumn` value.
- `ITableTranscoder` is renamed into `ITableAliaser`. Relates classes are also renamed (`MapTableTranscoder -> MapTableAliaser`, etc).
- `MapTableAliaser.queriedToUnderlying(...)` is renamed `MapTableAliaser.aliasToOriginal(...)`.
- Update `DuckDB` to `1.4.0.0` (from `1.3.2.0`).
- `AndFilterBuilder.filter` is renamed `AndFilterBuilder.and`.
- `OrFilterBuilder.filter` is renamed `OrFilterBuilder.and`.
- `TableQueryOptimizerSinglePerAggregator` now rely on `TableQueryOptimizer` to evaluate rootInducers.
- `ColumnFilter.isEqualTo` is renamed `ColumnFilter.matchEq`.

### Fixed
- Prevent `AdhocFactories` using an invalid `ICoordinateNormalizer` by default, using `StandardSliceFactory` own default.
- Prefer `array.concat(otherArray)` over `arrat.push(...otherArray)` to improve performance and prevent `max call stack size` (https://github.com/vuejs/core/issues/13826).
- `TableQueryEngineBootstrapped` will properly skip steps available from `IQueryStepCache`.
- Alternative `ITableQueryOptimizer` like `TableQueryOptimizerSinglePerAggregator` were broken due to lack of flexibility in sanity checks.

## [0.0.10]

### Added
- Introduced `ISliceFactory` to enable custom way of creating `Map` and `IAdhocSlice`.
- Introduced `ICoordinateNormalizer` to centralize how coordinates should be normalized (e.g. `int` into `long`).
- Introduced `NullMatcher.NULL_HOLDER` to represent `null` where a null-reference is forbidden.
- Introduced `ICalculatedCoordinate` to enable adding coordinate into columns. Typical usecase is having an `*` coordinate aggregating all other coordinates, as a grandTotal of current column.

### Changed
- Update `DuckDB` to `1.3.2.0` (from `1.3.1.0`)
- `ITabularRecord.getGroupBys()` now returns a `IAdhocMap`
- Most `SliceAsMap` usages are replaced by `IAdhocSlice`, and `IAdhocSliceWithStep` does not extends `IAdhocSlice` anymore.
- `IAdhocFilter.MATCH_ALL` is now serialized as `"matchAll"`
- `IAdhocFilter.matchAll` is now serialized as `"matchNone"`
- `IAdhocFilter` is renamed `ISliceFilter`

### Fixed
- Various mis-behaviours related to `null` as `GROUP BY` coordinate.

## [0.0.9]

### Added
- Columns can be tagged through `AdhocSchema`.
- Introduces `ICompactable`. It is called on each queryStep of the DAG.
- Introduces `ShiftedValueMatcher`, enabling `SimplerFilterEditor.shiftIfPresent` to shift dynamically existing values.
- Introduces `CumulatingDecomposition`, which is another case of `Dispatchor`. Can be used for cumulative `SUM` through time.
- Introduces `JooqTableQueryFactory.canFilterAggregates`, to generate `SUM(CASE y THEN x)` instead of `SUM(x) FILTER (y)`, as many Databases (like `RedShift` does not support `FILTER`).
- Introduced `JooqTableCapabilities` to configure the SQL capabilities on the underlying table.

### Changed
- `IColumnMatcher.toString()` has specialized implementations, depending on the matcher type (e.g. `c==v` for `EqualsMatcher`).
- `Bucketor` is renamed `Partitionor`.
- `StandardQueryOptions.CONCURRENT` is revamped for `CubeQuerySteps` by relying on a `RecursiveAction`.
- `DefaultCustomTypeManager` is renamed `StandardCustomTypeManager`.
- `DefaultMissingColumnManager` is renamed `StandardMissingColumnManager`.
- `DefaultQueryPreparator` is renamed `StandardQueryPreparator`.
- `DuplicatingDecomposition` is revamped regarding management of filtered-but-not-grouped-by columns.
- Update `DuckDB` to `1.3.1.0` (from `1.3.0.0`)
- `AdhocUnsafe.defaultCapacity()` is renamed `AdhocUnsage.defaultColumnCapacity()`
- `AtotiMeasureToAdhoc` now supports non-`String` properties.
- `MultitypeHashColumn.purgeAggregationCarriers()` now returns a purged copy, instead of mutating current instance.
- `InMatcher` and `ComapringMatcher` now normalize `long-like` into `long` and `double-like` into `double`.
- `AdhocPrimitiveHelpers.isLongLike` over `BigDecimal` should never returns true. `BigInteger` and `BigDecimal` are manage separately.

### Fixed
- `JooqTableQueryFactory` now supports `OrMatcher`, `AndMatcher` and `NotMatcher`.
- `ReversePolishCombination` now supports complex objects are subFormula results. More generally, subFormula results are are `.toString()` anymore.
- `StandardOperatorFactory.enrichOptions` now relies on some root `IOperatorsFactory`, especially relevant when using a `CompositeOperatorFactory`.

## [0.0.8]
### Added
- `CompositeTableTranscoder` is introduced.
- `IDecompositionEntry` is introduced.
- `IDagBottomUpStrategy` is introduced.
- `IDagBottomUpStrategy` is introduced.
- `AdhocFactories` is introduced.

### Removed
- `AtotiTableTranscoder.priorityTranscoder` is removed. Use `CompositeTableTranscoder` instead.
- `InMemoryTable_Recording` is removed. Use `InMemoryTable.throwOnUnknownColumn` instead.

### Changed
- `IAdhocColumn` now relies on `JsonTypeInfo.Id.MINIMAL_CLASS`
- Upgrade `DuckDB` to `1.3.0.0` (from `1.2.2.0`)
- Upgrade `SpringBoot` to `3.5.0` (from `3.4.6`)
- A not-Aggregator measure without underlying is automatically relying on the `empty` measure
- `FirstNotNullAggregation` renamed into `CoalesceAggregation`. Its key changed from `FIRST` to `COALESCE`
- `FirstNotNullCombination` renamed into `CoalesceCombination`. Its key changed from `FIND_FIRST` to `COALESCE`
- `LikeMatcher.like` renamed into `LikeMatcher.pattern`
- `AdhocUnsafe.failFast` renamed into `AdhocUnsafe.isFailFast()`
- `AtotiMeasureToAdhoc` should be extended with `@SuperBuilder` and a custom SuperBuilder.

## [0.0.7]
### Added
- `CachingTableWrapper` can be used to add a caching layer on top of a `ITableWrapper`. It can be skipped with `StandardQueryOptions.NO_CACHE`.
- `CompositeCubesTableWrapper` now supports `FILTER` statements, which are handled with a transient `Filtrator` measure.

### Changed
- `AdhocQuery` is renamed `CubeQuery`
- `AdhocQueryEngine` is renamed `CubeQueryEngine`
- `dag` sub-package is renamed `engine`
- `ExecutingQueryContext ` is renamed `QueryPod`
- Improved `JooqSnowflakeSchemaBuilder` to better handle edge cases like fields with a `.` in their name.
- `QueryPod` now relies on `MoreExecutors.newDirectExecutorService()` by default, which is used by not `StandardQueryOptions.CONCURRENT` queries.
- Rename `-Dadhoc.limitColumnLength=1024` into `-Dadhoc.limitColumnSize=1024`

### Fixed
- `TopologicalOrderSpliterator` is not `parallel` by default, as it should be triggered by `StandardQueryOptions.CONCURRENT`.

## [0.0.6]
### Fixed
- `SliceAsMap` did not handle a column with different `Comparable`, typically happening on `LocalDate` column with a `NULL` String on failed JOIN.
- `not` now works with `FilterHelpers.getFilteredColumns(IAdhocFilter)` and `JooqTableQueryFactory.toCondition(IAdhocFilter)`

### Changed
- `DuckDB` is upgraded to 1.2.2.0
- `QueryTableV2` introduced querySharing for `TableQuery` with same groupBy, by relying on SQL `FILTER` keyword.
- Drops `CharSequence` as a privileged type (e.g. in aggregations, in `IMultiTypeXXX`)

### Added
- Enable `cubeSlicer` in `CompositeCubesTableWrapper`: it enables filtering by underlying cube

## [0.0.5]
### Changed
- `StandardQueryOptions.CONCURRENT` has now much better behavior, especially around composite cubes, multiple tableQueries, and DAG with independent steps.
- Prepare querySharing for aggregators with same groupBy but different filters, with `FILTER` sql keyword. See `QueryTableV2`
- `ExpressionCombination` can refer to underlyings by their index with the syntax `underlyings[index]`

### Added
- Enable `customMarkerMetadata` in `AdhocSchema`
- Introduces `AdhocAutoConfiguration` for easy Spring auto-configuration.
- `AdhocQuery`s executed through `AdhocSchema` now see their `AdhocFilter` being transcoded (e.g. to manage a filter over a `date`, expressed as a JSON String).

### Removed
- Remove `StandardQueryOptions.RETURN_UNDERLYING_MEASURES` as it is meaningful only for aggregators and combinators

## [0.0.4]
### Changed
- `IQueryOption` is now part of `IAdhocQuery`
- `debug` and `explain` are now only togglable as `IQueryOption`
- Normalize the way columns are parsed into SQL names (by relying on JooQ `Parser.parseName`).
- Improve `.getColumns` relatively to various column customizations (e.g. customType, calculatedColumn, etc)
- Improve `EXPLAIN` by reporting the queried cube, the queryId and its parentQueryId.

### Added
- Enable complex filtering, by additional groupBy from table and manual post-filtering (e.g. `JooqTableQueryFactory.ConditionWithFilter`)
- Enable `CalculatedColumn` which can evaluate additional columns based on table columns.
- Introduce `IImplicitOptions` in `DefaultQueryPreparator`
- Initial work around `CONCURRENT` option.

## [0.0.3]
### Added
- `ForestAsGraphvizDag`: can turn an `IMeasureForest` into a Graphviz DOT file.
- `ICubeWrapper.getCoordinates` can query multiple columns in one call, which is leveraged by `DuckDb` tables.
- Fix serialization of `IQueryOptions`.

## [0.0.2]
### Added
- Enables m2central deployments: https://central.sonatype.com/artifact/eu.solven.adhoc/adhoc

## [0.0.1]
### Added
- Initial version
