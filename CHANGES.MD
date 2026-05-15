This document is intended for Adhoc developers and users.

It covers the core query engine and shared libraries. Pivotable-specific entries (the
SPA, the Pivotable backend, the dev stack, login flow, asset loading) live in
[pivotable/CHANGES.MD](pivotable/CHANGES.MD).

We adhere to the [keepachangelog](https://keepachangelog.com/en/1.0.0/) format


## [0.1.0]

Bump to 0.1.0 to mark the move to JDK 25 (still 0.x — Adhoc is not yet API-stable).

### Added
- New `GET /api/v1/cubes/queries/{queryUuid}/plan/summary`, `.../plan/snapshot` and `.../plan/children` endpoints — exposed in both **webmvc** (`PivotablePlanController`) and **webflux** (`PivotablePlanHandler`, wired through `PivotableApiRouter`). The `queryUuid` is the Pivotable-side UUID returned by `POST /cubes/query/asynchronous`; the engine now adopts the same UUID as its own `AdhocQueryId.queryId` (see `SubmittedQueryIdScope` below) so there is no second id to transcode. `summary` returns a cheap status object (state, node counts, `elapsedMs`, `startDelayMs`, latest-completed label) suitable for high-frequency polling; `snapshot` returns the full plan tree; `children` returns the composite-cube sub-queries' summaries. Response contract: **200** when the engine has registered a plan; **204 + `Retry-After: 1`** while Pivotable has the UUID but the engine hasn't yet registered (queueing / planning); **204** for any terminal "no plan to serve" state (evicted, unknown UUID, completed). **404 is reserved for "the endpoint itself does not exist"** — known endpoints never return 404 because a resource is absent; see [`docs/api.md`](docs/api.md) for the full HTTP-status convention. Sync queries have no LiveView by construction — by the time the SPA receives the response, the query is already done. Default Spring bean is `BoundedQueryPlanRegistry` (cap = 200 000 nodes), automatically wired into `CubeQueryEngine`. The engine flips `executionStartedAt` on the live source right before DAG execution begins, so `startDelayMs` reports the actual queueing delay when the executor pool is saturated. The async-query manager (`PivotableAsynchronousQueriesManager`) is now a shared Spring bean rather than a per-controller instance, so the query controller, plan controller, and async-result endpoint share the same "have you ever seen this UUID?" view.
- New `SubmittedQueryIdScope` (`engine/table`, package `eu.solven.adhoc.query`): JDK 25 `ScopedValue`–backed thread-scoped override for `AdhocQueryId.queryId`. A frontend that pre-generates a tracking UUID (e.g. Pivotable's async-query manager) binds it around the submission call; `AdhocQueryIds.from(...)` reads the binding inside `prepareQuery` — which runs synchronously on the submitting thread, before any future is scheduled — and threads it into the `AdhocQueryId` builder. Without a binding, the existing random-UUID default applies, so callers see no change. Mirrors the shape of `CustomMarkerScope` / `QueryOptionsScope`.
- New SPA component `adhoc-query-plan-live.js`: a small chip mounted next to the query grid (`adhoc-query.js`) that polls `/api/v1/cubes/queries/{queryUuid}/plan/summary` at 2 Hz once the async-query path has stashed a UUID into `tabularView.queryUuid`. Renders a Bootstrap badge with state + done/total counts + elapsed/wait time + last-finished-step label. Honours the four-state response contract above: shows "Queuing…" on `204 + Retry-After`, "Unknown query id" on `404`, "Plan no longer available (may have been evicted)" on `204` without Retry-After, and stops polling on terminal state (`DONE`/`FAILED`). The polling state-machine is extracted as a pure `nextPollState(prev, result)` reducer (Vitest-tested in `unit-tests/query-plan-live.spec.js`). End-to-end coverage in two specs: `localhost8080-query-plan-live.spec.js` asserts the badge surfaces a terminal "Done" state after a real async query against the WorldCupPlayers fixture and is absent before any query is submitted; `localhost8080-query-plan-live-states.spec.js` uses Playwright `page.route()` to intercept `/plan/summary` and pin each intermediate state (queuing on 204+Retry-After, unknown on 404, gone on 204 without Retry-After, recovery from 5xx).
- New `GET /api/v1/cubes/queries/{queryUuid}/plan/children` endpoint (webmvc + webflux): returns one {@link QueryPlanSummary} per registered child plan whose `parentQueryId` equals the supplied UUID. Empty array is the normal case for non-composite queries. Same four-state contract as the other plan endpoints (404 / 204 + Retry-After / 200 / 204) — see `PivotablePlanController` Javadoc for the table. Enables UI navigation over a composite query's fan-out without exposing the engine's internal `AdhocQueryId` shape to clients.
- Removed the deprecated push-side plan-registry path: `StaticPlanSource`, `QueryPlanRegistryUpdater`, and the entire `events/` package (6 event classes + `IQueryPlanEvent`). `IQueryPlanRegistry.register(QueryPlan)` is gone too — callers use `registerSource(IPlanSource)` directly. The pull-side `LiveQueryPlanSource` is now the single way the engine populates the registry. Reduces surface area before someone builds on the old shape.
- Pivotable SPA: TypeScript-via-JSDoc gate. Every `.js` source under `pivotable/js/src/main/resources/static/ui/js/` (78 files), every Vitest spec under `unit-tests/` (13 files), and every Playwright spec under `e2e-tests/` (23 files) now carries `// @ts-check` and passes `tsc --noEmit`. The six third-party libraries we use are typed against their real `.d.ts` files now: `vue-router`, `mermaid`, `bootstrap`, `slickgrid`, `sortablejs` (via `@types/sortablejs`), and `lodash` (via `@types/lodash`, re-exported under the `lodashEs` package alias). Each is installed at the same exact version we pin in the importmap, purely for type information — the runtime keeps loading them from WebJars/CDN as before. `pinia` remains under an `any`-shaped ambient declaration: enabling its strict types surfaces ~60 errors in `mapState` getter functions where `this` refers to the Vue Options-API component context (props/data) that pinia's types do not know about — re-enabling is parked until either the stores grow explicit state interfaces or the components migrate to the Composition API. Project-side ambient declarations remain in `pivotable/js/types/ambient.d.ts` for the `Window` augmentations (`Sortable`, `clickAddMeasure`, e2e `__adhoc*` diagnostic flags) and the `String.prototype.hashCode` extension. CI runs `npm run tsc_check` as a new step in the JS Unit Tests workflow before vitest. Zero runtime cost — files are served as raw `.js` exactly as before. Notable side-effects: replaced `new Date() - someDate` with `Date.now() - +someDate` (8 sites) so the type-checker accepts the arithmetic; `URLSearchParams({with_view: true})` became `{with_view: "true"}` (URLSearchParams accepts only string values); SlickGrid grid options typed as `Partial<GridOption>` so the literal-widened `rowTopOffsetRenderType: "top"` satisfies the union; `new SlickHeaderButtons()` gained an explicit `{}` options arg; the Pivotable-stamped extension `column.__menuItems` accessed via `/** @type {any} */` cast where SlickGrid's strict `Column<T>` rejects it. One pre-existing dead-code bug in `adhoc-query-grid-helper.js` (`for…in` reading `.id` off a string key) was annotated with `@ts-ignore` + `TODO` rather than rewritten — the loop has been silently emitting an empty `columnCss` map for a long time and a fix would surface a dormant cell-highlight feature without QA. New files inherit the gate.
- `RoutingMeasure` (`engine/recipes`, package `eu.solven.adhoc.measure.routing`): an `ICombinator`-style measure that dispatches each `CubeQueryStep` to one of several declared underlying measures via a caller-provided `Function<CubeQueryStep, String>`. Useful for migrations (typically Atoti → Adhoc) where a logical measure maps to different physical measures depending on the slice — e.g. one underlying for data before a modelling cutoff, another for data after. The function returns exactly one underlying name per step, so the framework cannot detect cross-boundary queries; callers must ensure their queries do not straddle the routing boundary, or accept silent dispatch to whichever branch the function chose. Because the function field is not Jackson-serializable, instances must be registered programmatically and cannot be declared in a measure-forest YAML/JSON resource file.

### Fixed
- Pivotable plan endpoints (`/cubes/queries/{uuid}/plan/*`) were never reachable in webmvc — `PivotablePlanController` was missing from `PivotableWebmvcSpringConfig`'s `@Import` block. Added, plus a `TestPivotablePlanEndpointsRouted` `@SpringBootTest` that hits each route over HTTP to catch the same regression shape in the future.
- Pivotable LiveView badge no longer reports "Queued" mid-flight: `LiveQueryPlanSource.markExecutionStarted` now flips `planState` from PENDING → RUNNING (previously it transitioned only to DONE/FAILED on completion).
- Pivotable SPA login chip ("You need to login") now opens the login modal in-place via `userStore.needsToLogin = true`, instead of navigating to `/html/login` (which caused a full document reload).
- Pivotable `/html/login` BASIC choice no longer triggers a full document reload: the route now renders `LoginProviders` with `:modal="true"` so the BASIC form is inlined rather than reached via an `<a href>` to `/html/login/basic`.
- Pivotable SPA plan-polling now uses `userStore.authenticatedFetch` (was plain `fetch`, which silently relied on cookie/session that the JWT chain rejects). 401 surfaces a one-shot "Log in to see the plan" hint, opens the login modal, and stops the poll loop instead of busy-looping on `Unauthorized`. Same fix on the Mermaid detail modal.
- Pivotable: `pivotable-server` ships IDE-autocomplete metadata for the env-read `adhoc.pivotable.login.oauth2.*` keys (master toggle + per-provider `enabled` / `clientId` / `clientSecret` for github & google). New `META-INF/additional-spring-configuration-metadata.json` carries the descriptions + default values; `spring-boot-configuration-processor` is wired as an `optional=true` annotation processor in the module's `pom.xml` (had to be added to `maven-compiler-plugin.annotationProcessorPaths` since the root pom declares an explicit list — implicit classpath processors are ignored when the list is set). On build, the processor emits the merged `META-INF/spring-configuration-metadata.json`, which IntelliJ Spring + VSCode Spring read for in-editor autocomplete and Javadoc on `application.yml` / `application.properties`. Future env-read keys can be added by appending to the same JSON file.
- Pivotable OAuth2 login-button override: a downstream user's `application.yml: adhoc.pivotable.login.oauth2.enabled: false` (or per-provider `.github.enabled: false` / `.google.enabled: false`) now disables the buttons even with the `pivotable-demo_external_oauth2` profile active. Previously the demo profile yaml carried `enabled: true` lines that silently overrode the user's base yaml (profile yaml beats base yaml in Spring Boot's precedence chain). Those lines were dropped from the yaml and kept as a commented-out reference block so the keys remain discoverable; the Java fallback in `PivotableLoginWebnoneController.loginProviders()` (`env.getProperty(P_OAUTH2, Boolean.class, true)`) encodes the same default-true behavior at the bottom of the precedence chain. Covered by 3 new regression tests in `TestPivotableLoginWebnoneController`.
- Pivotable: re-login on an existing query view no longer wipes the in-memory queryModel while the URL still carries the encoded model. Root cause: the model→URL watcher in `adhoc-query.js` writes via `history.pushState(...)`, which bypasses vue-router; vue-router's reactive `currentRoute.value.hash` therefore desynced from the real URL on every model edit. When `<AdhocQuery>` unmounted (wrapper swapped in `<LoginChip>` while `isLoggedIn` was false) and remounted post-login, the setup read the hash via `router.currentRoute.value.hash` and got vue-router's STALE / empty value — so `hashToQueryModel` hydrated nothing. New helper `queryHelper.readUrlHash(windowLike?)` reads `window.location.hash` directly (the authoritative source), URL-decodes it, and falls back to the raw form on malformed URIs; `adhoc-query.js` now calls it on the initial-hydration path. Covered by a new e2e test (`localhost8080-relogin-restores-query.spec.js`) that mimics the token-expiry → BASIC re-login flow and asserts the grid repopulates after re-login, and 5 new unit tests in `query-hash.spec.js`.
- Pivotable grid timings bar no longer grows indefinitely after a failed query (e.g. backend down). Previously the executor's POST would throw, the `catch` block surfaced the error to the UI, but `tabularView.timing.sending_startedAt` was left dangling — the renderer's `hasActiveTiming` / `formatTimings` then kept computing `Date.now() - startedAt` on every animation tick, growing the "sending" bar forever. A new pure helper `finalizeActiveStages(timing, now)` in `adhoc-query-grid-timings.js` walks every `<stage>_startedAt` marker, sets `<stage>` to the time-until-failure (clamped ≥ 0) and removes the marker; the executor calls it in its error branch. Same fix protects `executing_startedAt` / `downloading_startedAt` against errors raised mid-poll or mid-download. The `finally` block also now clears every loading flag (`executing`, `fetching`, `sleeping`) — previously only `sending` and `downloading` were reset, so a polling-loop error could leave the spinner stuck. Covered by 8 new unit tests in `query-grid-timings.spec.js`.
- Pivotable SPA: a handful of pre-existing dead-code references uncovered when enabling strict pinia types. `adhoc-cubes.js` references `store.contests` / `store.loadSchemas(...)` / `store.nbCubeFetching` that never existed on the adhoc store — the component is imported by `adhoc-endpoint.js` but its template never renders `<AdhocCubes>`, so the bug was silent; marked `@ts-nocheck` with a `TODO` until the file is rewritten or deleted. `adhoc-account-chip.js` and `adhoc-account.js` define a `player` getter that reads `store.players[this.playerId]` (neither field exists, and the getter is not referenced by either template); annotated with `@ts-ignore`-equivalent casts and a TODO. `adhoc-cube-header.js` referenced a non-existent `nbCubeFetching` in `mapState` and in the template's loading-spinner condition — dropped both. `adhoc-endpoints.js` mapped `needsToCheckLogin` from `useAdhocStore` but the key lives on `useUserStore` — split into two `mapState` calls. `adhoc-query-favorite.js` had `latestQueryIds >= 1` (comparing an array to a number — always false at runtime, so the surrounding `if` block was effectively dead); rewrote to `latestQueryIds.length >= 1` so the branch can actually fire.
- Pivotable chat: the WebFlux and WebMVC `PivotableChatConfiguration`s are now **always registered** regardless of whether an Anthropic API key is configured — the `@ConditionalOnProperty("adhoc.pivotable.chat.anthropic-api-key")` gate was removed. The SPA's `GET /api/v1/cubes/chat/enabled` probe now always returns `200` with a JSON body `{enabled: boolean, reason?: "NOT_CONFIGURED" | "DISABLED_BY_CONFIG" | "COOLDOWN", retryAfterSeconds?: number}`; the SPA hides the chatbot icon when `enabled === false` and logs the reason. `POST /api/v1/cubes/chat` returns `503` with the same JSON body when not currently usable. A new `enabled: true` toggle on `adhoc.pivotable.chat.*` allows turning the assistant off without scrubbing the API key. Replaces the prior behavior where the probe returned `404` whenever the API key was missing, polluting the browser console.
- Pivotable grid auto-fit (dblclick on a column-header resize handle): the footer row content (min/max summary, distinct counts) is now preserved when auto-fit runs. The previous implementation called `grid.updateColumnsInternal()`, which internally invokes `createColumnFooter()` and empties + recreates every footer cell — but the application's footer-population function (`gridHelper.updateFooters`) was not re-invoked, so the cells stayed empty. The new path manually rebuilds the per-column CSS rules and reapplies widths via `applyColumnHeaderWidths()` + `applyColumnWidths()` + `resizeCanvas()` without touching the header or footer DOM, so existing footer content persists and the update is cheaper.
- Pivotable grid phantom column (scroll-mode horizontal-scroll headroom): the resize handle is now hidden via `.slick-header-phantom .slick-resizable-handle { display: none }`. The phantom column exists only to provide scroll headroom and is not meant to be resizable; the visible handle was confusing.
- Pivotable chat: the WebFlux `PivotableChatConfiguration` was never imported by `PivotableWebfluxSpringConfig`, so the in-SPA chatbot endpoint (`POST /api/v1/cubes/chat`) had no route handler regardless of whether `adhoc.pivotable.chat.anthropic-api-key` was set — the SPA's `cubes/chat/enabled` probe always returned 404 and the chatbot UI permanently self-hid. The configuration is now `@Import`-ed in both `PivotableWebfluxSpringConfig` and (newly added) `PivotableWebmvcSpringConfig`, so the property gate becomes the only thing controlling activation.
- `CompositeCubesTableWrapper`: an `EmptyAggregation` aggregator is now forwarded to every sub-cube even when the sub-cube does not declare an explicit empty measure or a column matching the aggregator's column name. Previously, a mixed query — real measure + `Aggregator.empty()` — failed with `Measures [empty] are not known by any subCube` whenever no sub-cube happened to expose `empty` as a measure or as a column. Since an empty aggregator only materializes slices (no real value to aggregate), it can be served by any sub-cube unconditionally.
- `JooqTableQueryFactory.selectedRowsFields` (DRILLTHROUGH row-streaming path): `COUNT(*)` (an aggregator whose `columnName` is the literal `*`) is now rendered as `CASE WHEN <filter> THEN 1 END AS <alias>` instead of selecting the literal `*`. Previously, the wildcard reached the SELECT clause and JOOQ expanded it to every underlying column, causing per-aggregator alias values to bind to wrong columns (e.g. the `count(*)` alias would end up holding the `a` column's string value). The SLICES path already special-cased this via `DSL.count(...)`; the ROWS path now matches.

### Changed
- Pivotable grid: heatmaps are now split into two independent toggles, both **off by default**, in the Formatting Options modal. `primaryHeatmap` controls the whole-column green→red cell-background gradient calibrated against the column's min/max (via `heatmapColor()`); `secondaryHeatmap` controls the per-parent-group in-cell bar calibrated against the parent slice (via `secondaryHeatmapFill()`, only meaningful when the query has a hierarchical groupBy). On a fresh load the grid reads like a plain spreadsheet; flipping either toggle restores that side of the previous visual emphasis. When OFF, the measure/percent formatters short-circuit before the respective helper call, so disabled features cost nothing.
- Pivotable SPA: cross-layer library-version alignment is now enforced. (a) New `pivotable/js/unit-tests/lib-version-alignment.spec.js` reads `pom.xml` (the source of truth), every importmap (`importmap-{webjars,cdn}{,-min}.json`), `vite.config.js`'s `IMPORTMAP_ALIASES`, and `package.json` for each tracked library (lodash-es, mermaid, slickgrid, sortablejs, vue, vue-router, pinia, bootstrap), and asserts every version segment matches — a partial bump (e.g. Renovate touching only `pom.xml`) now fails CI before merge. (b) `renovate.json` adds per-library `groupName`s so future Renovate PRs bundle the WebJar + npm + types updates into one PR (lodash, mermaid, slickgrid, sortablejs, vue, vue-router, pinia, bootstrap). Adding a new library: append one entry to `TRACKED` in the spec file and one packageRule to `renovate.json`.
- Pivotable SPA: bumped `lodash-es` WebJar from `4.17.21` to `4.18.1` across `pom.xml`, all four importmaps, `vite.config.js`'s `IMPORTMAP_ALIASES`, and the new alignment unit-test fixture. (The npm devDep `lodashEs: npm:lodash@4.18.1` was already at 4.18.1.) Renovate PR #778 had attempted only the `pom.xml` half of this bump.
- Pivotable SPA: bumped `mermaid` from `11.6.0` to `11.14.0` (the latest WebJar published; 11.15.0 exists on npm but has no matching WebJar yet) across all four loading modes — `pivotable/js/pom.xml`, the four importmaps (`importmap-{webjars,cdn}{,-min}.json`), `vite.config.js`'s `IMPORTMAP_ALIASES`, the npm devDep (for type checking), and the `index-html-default-mode.spec.js` alignment assertion. No source-level usage change (`mermaid.initialize(...)` + `mermaid.render(...)` API is stable across 11.x).
- Pivotable grid auto-fit (dblclick on a column-header resize handle): cell width is now measured via Canvas2D `measureText` on the formatter output instead of `cell.scrollWidth`. The DOM-based path returned the cell's CURRENT rendered width when content fit comfortably, so an already-too-wide column would auto-fit to its own current width and never shrink. Canvas measurement reports the natural content width, independent of the rendered cell width. The header chrome reservation also grew (14 → 42 px) to include the on-hover 3-dot action menu icon, so the auto-fit width never under-shoots the header when the user hovers it.
- Pivotable grid column-resize handle: now symmetric around the column boundary. The SlickGrid Alpine theme ships a geometrically symmetric handle (`right: -4px, width: ~8px`, 4 px each side) but `.slick-header-column { overflow: hidden }` was clipping the right half both visually and for hit-testing — the user could only grab the strip from the left side of the boundary. Lifting the parent's overflow restores the right-side hit area; the handle width itself is unchanged, so it does not overlap the next column's name. Column-name truncation still works because the inner `.slick-column-name` has its own ellipsis.
- Pivotable grid: empty cells are now rendered with explicit greyed-italic placeholders so users no longer have to guess whether a blank cell means "null", "empty string", or "rolled-up total". `null`/missing values render as `NULL`, `""` renders as `empty`, the engine-side grand-total marker `*` (CalculatedCoordinate) renders as `Total`. The DRILLTHROUGH-only `Out of DT` placeholder still flags columns the user did not request. Applies to groupBy, measure, and percent formatters in every view (not just DRILLTHROUGH). The query payload is unchanged — `*` remains the on-the-wire marker; only the display label is rewritten.
- `QueryStepsDagBuilder.rootMeasureless` (engine/table): when a `ColumnWithCalculatedCoordinates` declares calculated coordinates whose names collide with real coordinate values present in the underlying table (e.g. a calc-coord `blue` on a `color` column that already has a `blue` row), the natural sub-query for that column now receives a `NOT IN (<calc-coord names>)` filter. The calculated coordinate becomes authoritative for its slice key — without this, the two rows would clash on merge in `MapBasedTabularView.appendSlice` and the engine would crash with `IllegalArgumentException: Multiple entries with same key`. The legacy grand-total marker `*` is excluded from the suppression set: it never overlaps with real data and emitting `col != "*"` against a typed column (DATE, INTEGER, …) crashes the SQL backend on type coercion (DuckDB: "invalid date field format").
- New `TestPivotableMcpServer_SseRoundTrip` (pivotable/mcp): end-to-end test that boots a Spring Boot WebFlux app on a random port with the `pivotable-mcp` profile active, connects an in-process `McpSyncClient` via `WebFluxSseClientTransport` to `/mcp/sse`, performs the MCP `initialize` handshake, lists tools, and invokes them. Complements the existing logic-level (`TestPivotableMcpTools`) and bean-wiring (`TestPivotableMcpConfiguration`) tests by exercising the SSE transport, JSON-RPC framing, and Spring AI `@Tool` → MCP-protocol bridge — no AI agent required.
- Pivotable SPA: "Copy MCP token" affordance under the logged-in account chip (`adhoc-account-me-loggedin.js`). Surfaces the current `access_token` (the same JWT the SPA already uses for `/api/**`) as a clipboard-ready `Authorization: Bearer <jwt>` line so users can paste it into an MCP client's `.mcp.json` `headers.Authorization`. The MCP server's `/mcp/**` routes share the existing `PivotableJwtWebfluxSecurity` chain — no separate auth config is needed. A `<details>` block shows the raw token as a fallback when the clipboard API is unavailable (insecure context, denied permission). Short-lived by nature — proper long-lived API-token / OAuth 2.1 flow is on the ROADMAP.
- New `pivotable-mcp-webflux` and `pivotable-mcp-webmvc` starter modules (essentially empty JARs whose value is the transitive dependencies): each bundles `pivotable-mcp` (transport-agnostic `@Tool` definitions) with the matching Spring AI server starter (`spring-ai-starter-mcp-server-webflux` / `spring-ai-starter-mcp-server-webmvc`). The Pivotable WebFlux and WebMVC server modules now depend on the appropriate starter and `@Import` `PivotableMcpConfiguration`; the configuration class is gated on `@Profile("pivotable-mcp")` so the wiring is a no-op until the profile is active. Activating it via `SPRING_ACTIVE_PROFILES=…,pivotable-mcp` now actually exposes `/mcp/sse` (which it did not before — the MCP module shipped on the classpath but was never `@Import`-ed by either server application).
- New `docs/mcp-integration.md`: walkthrough of the MCP feature for downstream projects — what is exposed (three `@Tool` methods), how to wire it (Maven dep + Spring profile), how to connect a client (Claude Code `.mcp.json` entry or programmatic Java client), and the three layers of test coverage. Now also covers the (separate) Pivotable Chat assistant — how to enable it via `ADHOC_PIVOTABLE_CHAT_ANTHROPIC_API_KEY`, the simplified SSE event vocabulary the SPA consumes, and the WebFlux/WebMVC implementation split. A "Generating a new access_token" subsection documents both the SPA "Copy MCP token" interactive flow and the programmatic `GET /api/v1/oauth2/token` refresh exchange.
- Pivotable chat now works under both WebFlux and WebMVC. The framework-agnostic prompt assembly and Anthropic-SSE → simplified-SSE translation moved into `pivotable-server-core` (`ChatRequestPlanner` + `AnthropicSseTranslator`); the WebFlux `PivotableChatHandler` delegates to them, and a new WebMVC `PivotableChatController` (`pivotable/server-webmvc/.../chat/`) provides the servlet-side mirror using the JDK `HttpClient` plus `SseEmitter` pumped on the application task executor. Wire contract is identical between the two stacks (`GET /api/v1/cubes/chat/enabled` → 204; `POST /api/v1/cubes/chat` → text/event-stream of `text` / `tool_use` / `done` / `error` events). Default model bumped from `claude-haiku-4-5-20251001` to `claude-haiku-4-5`; override via `adhoc.pivotable.chat.model`.
- New `CustomMarkerScope` and `QueryOptionsScope` (libraries/dataframe, package `eu.solven.adhoc.factories`): thread-scoped access to the executing query's `customMarker` and `Set<IQueryOption>` via JDK 25's `ScopedValue`. `CubeQueryEngine#execute` binds both around the whole call (planning *and* row processing); extension points like `ICalculatedColumn#computeCoordinate` and `ICalculatedCoordinate#getFilter` can read them via `CustomMarkerScope.current()` / `QueryOptionsScope.current()` (plus the convenience `QueryOptionsScope.isActive(option)`) without any API widening. The motivating use case for `QueryOptionsScope` is a complex calculated column that performs a DB round-trip and needs to honour `NO_CACHE` / `EXPLAIN`. Composite-cube fan-out re-enters `execute` per sub-cube, so each sub-query naturally rebinds. Outside a `CubeQueryEngine#execute` call, `current()` returns empty (`Optional.empty()` / empty `Set`) rather than throwing.

### Added
- New `CubeColumnsWrapper` (`engine/cube`, package `eu.solven.adhoc.cube`) owns the cube's column-resolution subsystem on behalf of `CubeWrapper`: collects table columns + calculated columns + generated columns and attaches caller-facing aliases. The result is memoised; `invalidateAll()` swaps in a fresh memo, so a downstream schema change is picked up on the next `getColumns()` call. Caching also dedupes the `Discarding alias=…` warnings (one per cube lifetime instead of once per call). `CubeWrapper` now implements `IHasCache`; its `invalidateAll()` propagates to the underlying `ITableWrapper` when the table is itself an `IHasCache`. `CompositeCubesTableWrapper` also implements `IHasCache` and fans out to every sub-cube.

### Changed
- Aggregator restructuring: the `adhoc` and `adhoc-table` Maven modules are now nested under a new `engine/` aggregator pom (`adhoc-engine`). The `adhoc-table` folder has been renamed `engine/table/`; the former `engine/adhoc/` folder has been further renamed `engine/recipes/` (artifactId `adhoc` → `adhoc-recipes`, automatic-module-name `eu.solven.adhoc` → `eu.solven.adhoc.recipes`) to better reflect its role as a home for advanced-measure recipes and example datasets on top of the engine layer rather than as the engine itself. Downstream consumers of the artifact must update their `<artifactId>` from `adhoc` to `adhoc-recipes`.
- `adhoc-model`: every package now declares `@NullMarked` (JSpecify) via `package-info.java` — parameters, return types and fields are non-null by default; explicit `@Nullable` opt-outs are required for legitimately-nullable references.
- NullAway flow-sensitive nullness analysis is now wired into the `errorprone` profile (opt-in via `-Perrorprone`). It runs as an Error Prone plugin in `JSpecifyMode=true` with `OnlyNullMarked=true`, so only packages annotated with `@NullMarked` are checked — the rest of the codebase stays silent until each package opts in. Severity is `ERROR`; CI's `static-analysis` workflow (which already passes `-Perrorprone`) will start failing on any unsafe deref / unsafe pass / wrong return inside a marked package. NullAway 0.13.4 (compatible with Error Prone 2.49+).
- `adhoc-table` (engine/table): every package now declares `@NullMarked` (JSpecify) and the module passes NullAway clean. Notable contract changes:
  - `IIsCancellable#getCancellationDate()` returns `@Nullable OffsetDateTime` — matches the existing `return null` paths in `StandaloneTableQueryPod` and `QueryPod` ("no scheduled cancellation").
  - `ICustomMarkerCacheStrategy#restrictToCacheImpact(Object)` accepts and returns `@Nullable Object`. The `customMarker` field on `TableQuery`, `TableQueryV2`, `TableQueryV3`, `TableQueryV4` is `@Nullable` (matches `@Default Object customMarker = null`); cascaded through `CachingTableWrapper.makeCacheKey` / `mergeAggregates` and `TableStepsGrouper.contextOnly`.
  - `JooqTableQueryFactory.toSqlAggregatedColumn`, `SliceToJooqCondition.onCustomCondition`, `DuckDBHelper.getCoordinates(table, column, …)` return `@Nullable` (matches existing `return null` for the empty-aggregation, no-translation, and missing-column cases).
  - `PrunedJoinsJooqTableSupplier.columnToAlias` / `fullTableCache` typed `@Nullable` to match the lazy-init / invalidate-to-null patterns.
  - Lombok `@Builder` cases (`ColumnWithCalculatedCoordinates`, `TableQueryFactory`, `TableQueryV4`, `JooqTableQueryFactory` (SuperBuilder), `JooqTableWrapperParameters`, `StandardDSLSupplier`, `JooqJoinBuilder`) carry `@SuppressWarnings("NullAway.Init")` on the inner builder class (or class-level for SuperBuilder/manual variants).
  - `engine/table/pom.xml` jacoco thresholds were lowered from 0.70/0.78 to 0.50/0.55 to reflect the v19 module split (most tests still live in `engine/recipes` — formerly `engine/adhoc`).
- `adhoc-dataframe`: every package now declares `@NullMarked` (JSpecify) and the module passes NullAway clean. Notable contract changes:
  - `ITableAliaser#underlying(String)` was already `@Nullable` — every implementation (`IdentityImplicitAliaser`, `MapTableAliaser`, `MapTableStrictAliaser`, `RecursiveAliaser`, `CompositeTableAliaser`) now matches.
  - `ITabularRecord#getAggregate(String)` returns `@Nullable Object`; cascaded through `TabularRecordOverMaps`, `HideAggregatorsTabularRecord`, `TabularGroupByRecordOverMap`.
  - `AAggregatingColumns#getColumn(String)` and the `IAliasedAggregator` overload now return `@Nullable IMultitypeColumn<K>` — matches the existing `aggregatorToAggregates.get(...)` lookup pattern in `AggregatingColumns` / `AggregatingColumnsDistinct`.
  - `AdhocPrimitiveHelpers.asLong/asDouble`/`normalizeValueAsProvider` accept `@Nullable Object` (with explicit `requireNonNull` for the as-primitive variants — preserves the prior NPE-on-null contract while letting callers flow nullable values through).
  - `PerfectHashMap.Builder#append(Object)` accepts `@Nullable Object` (the implementation already mapped null to the ABSENT sentinel).
  - `ChunkedList`/`ChunkedDoubleList`/`ChunkedLongList`: `head`/`tail` arrays are typed `@Nullable` to match the lazy-init pattern; `readAt`/`writeAt`/`compact` narrow with `Objects.requireNonNull` against the "callers pass index < size" invariant.
  - `IValueReceiver#onObject` overrides aligned in 12 dataframe-side files (`MultitypeArray`, `MultitypeColumnHelpers`, `Multitype{Hash,Navigable,NavigableElseHash}{,Int}Column`, `CleaningValueReceiver`, `Merging{Column,IntColumn,Navigable}ValueReceiver`, `AMultitypeNavigableElseHashColumn`).
  - Lombok `@Builder` cases (`MapTableAliaser`, `TabularRecordOverMaps`) carry `@SuppressWarnings("NullAway.Init")` on the inner builder class.
- `adhoc-measures`: every package now declares `@NullMarked` (JSpecify) and the module passes NullAway clean. Notable contract changes:
  - `IAggregation#aggregate(Object, Object)` accepts and returns `@Nullable Object` — every implementation aligns (`SumAggregation`, `AvgAggregation`, `CountAggregation`, `MaxAggregation`, `RankAggregation`, `ProductAggregation`, `CoalesceAggregation`, `EmptyAggregation`, `ExpressionAggregation`, `LambdaAggregation`, `MapAggregation`, `UnionAggregation`, `UnionListAggregation`, `UnionSetAggregation`, `AtomicLongMapAggregation`); matches the existing contract that any input or aggregate may legitimately be `null`.
  - `ICombination#combine(ISliceWithStep, List<?>)` returns `@Nullable Object` (matches existing `return null` paths in `CoalesceCombination`, `DivideCombination`, `EvaluatedExpressionCombination`, `ReversePolishCombination`, `SubstractionCombination`).
  - `IAggregationCarrier.IHasCarriers#wrap(Object)` returns `@Nullable IAggregationCarrier` (matches `CountAggregation.wrap` returning null on a 0-count, and `RankAggregation.wrap` returning null on null input).
  - `AdhocMapHelpers.aggregateMaps(Map<?,?>, Map<?,?>)` accepts and returns `@Nullable` (cell module — moved from non-null to match real call-site behaviour).
  - `IAppendableColumn#append(Object)` and `ITableRowWrite#add(String, Object)` accept `@Nullable Object` value (encoding module — matches actual call sites which already pass null). Cascaded through every column implementation.
  - `IInt2ObjectReader` overrides in `MapOverIntFunction` (cell) declare `@Nullable Object read(int)` to match the interface; the wrapping `getSequencedValueRaw` is enforced non-null with `Objects.requireNonNull` (callers rely on the NULL_HOLDER sentinel rather than real null references).
  - `AbstractAdhocMap.getSequencedValue` / `getSortedValue` / `entrySet()`-iterator value: `@Nullable Object` returns to match the `unwrapNull(NULL_HOLDER) → null` contract.
  - Lombok `@Builder` cases (`UnsafeMeasureForest`, `Aggregator`, `Combinator`, `Shiftor`, `Unfiltrator`) carry `@SuppressWarnings("NullAway.Init")` on the inner manual builder class.
- `adhoc-encoding` and `adhoc-filters`: every package now declares `@NullMarked` (JSpecify) and both modules pass NullAway clean. Notable contract changes:
  - `IInt2ObjectReader#read(int)`, `ITableRowRead#readValue(int)` and `IReadableColumn#readValue(int)` (and every override) declare `@Nullable Object` returns to match the documented "returns null on miss / on absent value" semantics. Affected impls: `TableRowHelpers.EMPTY_ROW_READ`, `AppendableTablePage.TablePageRowRead`, `ObjectArray*Column`, `IntegerArrayColumn`, `LongArrayColumn`, `Dictionarized*Column`, `FsstReadableColumn`, `DynamicReadableColumn`.
  - `IAppendableTablePage#pollNextRow()` was already `@Nullable`; the override on `AppendableTablePage` now matches.
  - `PerfectHashMap` and `MutablePerfectHashMap`: `get`, `put`, `remove` declare `@Nullable V` returns to match `Map<K, V>#get/put/remove` semantics; the inner-iterator `computeNext()` overrides also `@Nullable` to match Guava's `AbstractIterator` `endOfData()`.
  - `AppendableTablePage.columnsWrite` is now `AtomicReference<@Nullable List<IAppendableColumn>>` (the field is intentionally nulled at freeze time); call sites narrow with `Objects.requireNonNull` (write phase) or already null-check (read phase).
  - `IValueMatcher#match(Object)` and every implementation (`AndMatcher`, `OrMatcher`, `NotMatcher`, `EqualsMatcher` family, `InMatcher`, `LikeMatcher`, `RegexMatcher`, `SameMatcher`, `String*Matcher`, `NullMatcher`, `ComparingMatcher`) now accept `@Nullable Object` — matches the existing Javadoc ("value may be null") and the way callers (notably `ColumnFilter.toString`) actually use it.
  - `EqualsMatcher.matchEq(Object)` and `EqualsMatcher.createWrapped(Object)` declare `@Nullable Object` parameter.
  - `NullMatcher.unwrapNull(Object)` declares `@Nullable Object` on both parameter and return — matches the unwrap-the-NULL_HOLDER-back-to-null contract.
  - `AdhocPrimitiveHelpers.isLongLike(Object)` / `isDoubleLike(Object)` accept `@Nullable Object` (their `instanceof` checks already return `false` on null).
  - Lombok `@Builder` cases (`ColumnFilter`, `ComparingMatcher`, `CollectionWithCustomIndexOf`) declare `@SuppressWarnings("NullAway.Init")` on the inner builder class.
  - `MaskedAdhocMap.retainAll`, `ColumnPacker.packColumnFilters`, `FilterOptimizer.removeLaxerOrStricter`, `KernelFactorizer` factor out two-keys-always-populated `partitioningBy` results into `Objects.requireNonNull` locals — turns the implicit "Collectors.partitioningBy populates both true and false" invariant into something NullAway can verify.
- `adhoc-cell`: every package now declares `@NullMarked` (JSpecify) and the module passes NullAway clean. Notable contract changes:
  - `ICoordinateNormalizer#normalizeCoordinate(Object)` accepts and returns `@Nullable` (the standard implementation already maps `null` to `NULL_HOLDER`, so the runtime behaviour is unchanged). `ASliceFactory#normalizeCoordinate` and `StandardCoordinateNormalizer#normalizeCoordinate` align.
  - `IMapBuilderPreKeys#append(Object)` now accepts `@Nullable` (its callers — `Slice`, the `optional Iterable<?>` overload, …  — already passed null on purpose). Implementations in `ColumnSliceFactory.MapBuilderPreKeys` and `RowSliceFactory.MapBuilderPreKeys` align.
  - `Slice#getGroupBy(String)` and `MaskedAdhocMap#get(Object)` / `AbstractAdhocMap#get(Object)` declare `@Nullable Object` returns to match `Map#get` and `ITabularGroupBySlice#getGroupBy` semantics.
  - `MaskedAdhocMap#retainAll`: replaced two pairs of `partitionedColumns.get(true)` / `.get(false)` with locals narrowed via `Objects.requireNonNull` (the partitioning collector always populates both keys; the get-then-deref pattern was hiding that invariant from the type system).
  - Lazy caches `AbstractAdhocMap.entrySet`, `SequencedSetLikeList.list` are now declared `@Nullable` to match their lazy-init semantics.
  - Lombok `@Builder` cases (`CubeQueryStep`, `AdhocLogEvent`, `ColumnSliceFactory.MapBuilderPreKeys`) carry `@SuppressWarnings("NullAway.Init")` on the (manual or implicit) inner builder class so NullAway tolerates the chained-setter init pattern it can't analyze.
- `adhoc-model` now passes NullAway clean. Notable contract changes surfaced by the analysis:
  - `IGroupBy#getColumn(String)` now throws `NoSuchElementException` when no column with that name is part of the groupBy (previously returned `null` silently, which was misleading and incompatible with `@NullMarked`).
  - `IValueReceiver#onObject(Object)`, `IThrowingValueReceiver#onObjectMayThrow(Object)`, `IValueProvider#getValue(IValueProvider)`, `IValueProvider#setValue(Object)`, `AdhocPrimitiveHelpers#normalizeValue(Object)`, `AdhocDebug#toString(Object)` and `IValueProviderTestHelpers#getObject(IValueProvider)` are now declared `@Nullable` on the parameter (and/or return) — matches what the docstrings already promised and what call-sites already relied upon.
  - `CacheEntry<K,V>` (`util.cache`) now exposes its `key`/`value` fields and constructor parameters as `@Nullable` — the sentinel "first call always misses" entries pre-existed in the constructors of `LastLookupCache`, `LastLookupCache1`, `LastLookupCache1Volatile` and `LastLookupCache1Scoped`, and the `getByRef(...)` returns are now `@Nullable V` to match the documented "returns null on miss" contract.
- `adhoc-recipes` (engine/recipes — formerly `adhoc` / engine/adhoc): every package now declares `@NullMarked` (JSpecify) and the module passes NullAway clean. Notable contract changes:
  - `CubeQuery#customMarker`, `MeasurelessQuery#customMarker`, `IFilterEditor.FilterEditorContext#customMarker` and `IHasCustomMarker#getCustomMarker()` (model) are now `@Nullable Object` — matches the existing `@Default Object customMarker = null` and the existing `return null` paths.
  - `AdhocSubQuery#getCustomMarker()` and `ICustomMarkerTranscoder#transcodeCustomMarker(ICubeWrapper, Object)` accept and return `@Nullable Object`. `CubeWrapperTypeTranscoder#toTable(String, Object)` returns `@Nullable Object` (transcoder is allowed to map a known coordinate to `null`).
  - `IIsCancellable#getCancellationDate()` (cell) returns `@Nullable OffsetDateTime` (the `AtomicReference` stays `null` until cancellation). `IQueryStepCache#pushValue(...)` accepts a `@Nullable SizeAndDuration` (per-step duration is optional).
  - `IColumnValueTranscoder#transcodeValue(String, Object)`, `ICustomTypeManagerSimple#toTable(String, Object)` and `ICustomTypeManager#fromTable(String, Object)` (dataframe) accept and return `@Nullable Object` — every implementation aligns (`StandardCustomTypeManager`, `ColumnsManager`'s inner transcoder, `EventAggregation`).
  - `ACustomMarkerCombination#combine(...)` and `ACustomMarkerCombination#getDefault()` return `@Nullable Object`. `ShiftorQueryStep#shift(ISliceFilter, Object)` accepts a `@Nullable` customMarker.
  - `DuplicatingDecomposition#getDefaultCoordinates(String)` and the `JoinDecomposition#toStringList(Object, ...)`/`ManyToMany{1D,ND}Decomposition` group-callbacks align with `IValueMatcher#match`'s `@Nullable` parameter — call sites narrow with `Objects.requireNonNull` against the "null already filtered out" invariant.
  - `QueryStepsDagBuilder.pollLeftover()` returns `@Nullable CubeQueryStep`; `MeasureForests#getForest(String)` returns `@Nullable IMeasureForest` — call sites narrow with `Objects.requireNonNull` against the explicit `hasLeftovers()` / known-name preconditions.
  - Multiple `Map.get`-with-known-key sites in `AdhocSchema` (nameToCube/nameToTable lookups), `CubeWrapperTypeTranscoder` (columnToTypes), `CompositeCubesTableWrapper` (nameToCube — 3 sites) and `DuplicatingDecomposition.getCoordinatesAlongColumn` (columnToCoordinates) are narrowed with `Objects.requireNonNull` so NullAway can verify the contract.
  - `FiltratorQueryStep` and `QueryStepsDagBuilder` resolve the optional `IFilterOptimizer` via `Objects.requireNonNullElse(TransverseCacheHelper.getFilterOptimizer(...), AdhocFilterUnsafe.filterOptimizer)` instead of relying on `FilterBuilder.optimize(null)`. `AdhocJackson` and `WorldCupPlayersSchema` narrow reflection / `ClassPathResource#getFilename()` results with `Objects.requireNonNull` (descriptive messages).
  - Lombok `@Builder` cases (`CubeQuery`, `CubeWrapper`, `QueryPod`, `MeasurelessQuery`, `IFilterEditor.FilterEditorContext`) carry `@SuppressWarnings("NullAway.Init")` on the inner builder class. `CubeQueryBuilder.customMarker(@Nullable Object)` carries `@SuppressWarnings("NullAway")` because Lombok's generated `customMarker$value` builder field doesn't inherit the `@Nullable` annotation from the source field.

## [0.0.19]

### Added
- `StandardQueryOptions.DRILLTHROUGH`: now production-ready (un-deprecated). All `TableQueryV4`s of the query DAG are merged into a single covering query (new `eu.solven.adhoc.engine.tabular.optimizer.TableQueryV4Merger`: OR-combined WHERE filter, non-ambiguous union groupBy, deduplicated and re-aliased aggregator union — same-name aggregators with distinct per-aggregator `FILTER`s become `k1`, `k1_1`, …). The merged query is executed by a dedicated `TableQueryEngine#executeDrillthrough` path that streams every table row directly into a `ListMapEntryBasedTabularViewDrillThrough` — one entry per source row, with the merged groupBy as `coordinates` and the per-aggregator (aliased) values as `values`, bypassing per-slice aggregation and any cube-level measure transformation. A `SKIPPED_CELL = "$adhoc.drillthrough.skipped_cell"` constant is reserved for future heterogeneous schemas (currently the merged query produces a uniform schema). Pivotable: the option is surfaced in the wizard's options panel and the per-cell modal gains a "DrillThrough this cell" button that pins the row's coordinates as filters, enables the option and asks the user to Submit.

### Changed
- `ForestAsMermaidDag` (new `eu.solven.adhoc.measure.mermaid` package in the `experimental` module): Mermaid-flowchart counterpart of `ForestAsGraphvizDag`. Renders an `IMeasureForest` (or a `MeasureForests`, using one `subgraph` block per forest) as `flowchart LR` text, using the same per-class shape/color conventions as the Graphviz sibling and as the interactive Pivotable measure-graph modal (`adhoc-measures-dag.js`). Supports a `highlightedMeasures` builder option (thick red border). Intended for docs and for previewing programmatically built forests — the output can be pasted at https://mermaid.live or rendered inline by any Mermaid-capable Markdown viewer.
- `eu.solven.adhoc.measure.ThrowingCombination`: promoted from `libraries/measures` test scope to main scope so downstream projects (notably Pivotable's `simple` cube) can register it to exercise error-management paths. Behaviour unchanged — still throws `ThrowingCombinationException("Throwing on slice=…")` on every `combine` call. Existing in-module tests (`TestTransformator_Combinator_Exception`, `TestTransformator_Partitionor_ExceptionAsMeasure`, `TestCubeQueryEngine`) keep working unmodified.

### Fixed
- `TableQueryEngine#suppressGeneratedColumns`: when a query groups by a column that is both (a) produced by an `IColumnGenerator` (e.g. the `scenarioIndex` column of `ExampleVaRDecomposition`) AND (b) carries a `withStar`/`ColumnWithCalculatedCoordinates` grandTotal, the engine used to suppress BOTH the plain `ReferencedColumn` branch AND the synthetic `FunctionCalculatedColumn` branch — collapsing them to the same `grandTotal` table query and then restoring the same `COORDINATE_GENERATED="generated"` coordinate for both. The two branches then emitted the same slice key with the same measure value, crashing `MapBasedTabularView.appendSlice` with `IllegalArgumentException: Multiple entries with same key`. Fix: suppression now skips any groupBy column whose `IAdhocColumn` is an `ICalculatedColumn` — calculated columns carry their own value-computation logic and must not be overwritten by the generator fallback. Reproducer (and new regression test): `TestTableQuery_DuckDb_VaR#testGroupByScenarioIndex_withStar_countAsterisk` (plus `…_withStar_mArray` for the companion case where the measure does decompose).
- `ForestAsGraphvizDag.highlightedMeasures`: new builder option receiving a `Set<String>` of measure names. Matching nodes receive a thick red double-perimeter border (`color="red"`, `penwidth="3"`, `peripheries="2"`) on top of their regular shape/fill styling, so a human can quickly locate them in a large DAG.
- `IHasExecutorAndSliceFactory` + `PodExecutors` + `IScopeBinder`: new marker interface implemented by `IAdhocFactories` and `QueryPod` exposes both the executor service and the slice factory, and a new `PodExecutors` utility (`runScoped`, `callScoped`, `submitScoped`, `scopedExecutor`) centralises scope-binding around dispatched tasks. `ISliceFactory`'s `callWithScope` / `runWithScope` / `getWithScope` methods are deprecated in favour of having `ColumnSliceFactory` implement `IScopeBinder#bindScope` directly; `CubeQueryEngine#execute`, `PartitionorQueryStep#produceOutputPerInputPartition`, and `TabularRecordStreamReducer` now dispatch through `PodExecutors`.
- `PerfectHashMap` / `PerfectHashKeyset`: immutable `Map<String, V>` backed by a reusable `PerfectHashKeyset` (an ordered `SequencedSet<String>` of key fields wrapping a `SimplePerfectHash`) and an `ImmutableList<V>` of values. The keyset (and especially the perfect-hash table) is computed once and shared across many maps; each map only pays for an `ImmutableList` sized to `keyFields.size()`. A public `ABSENT` sentinel encodes `not in this map` so that the values list can stay in `ImmutableList` (which forbids nulls). `PerfectHashMap.keySet()` returns the shared `PerfectHashKeyset` by reference when the map is fully populated. Intended for callers like `TabularRecordBuilder#aggregates` that build many maps with the same key set.
- `ScopedValueAppendableTable`: JDK-25 `ScopedValue`-backed analogue of `ThreadLocalAppendableTable`. Each caller thread establishes its own page-map via `runInScope(Runnable)` / `callInScope(Callable)`; `nextRow` outside a scope throws `NoSuchElementException`. Intended for faster per-request access on Virtual Threads versus `ThreadLocal.get()`.
- `ScopedValueAppendableTableFactory`: opt-in alternative to `ThreadLocalAppendableTableFactory` that returns a `ScopedValueAppendableTable`. Wired end-to-end via new `ISliceFactory.callWithScope` / `runWithScope` / `getWithScope` (no-op default; `ColumnSliceFactory` delegates to `ScopedValueAppendableTable.callInScope` when backed by one). Dispatch sites now establish a scope per virtual-thread task: `CubeQueryEngine.execute` wraps the DAG execution (covers sequential path + composite post-processing + sub-cube re-entry), `PartitionorQueryStep.produceOutputPerInputPartition` wraps each submitted `Callable`, and `TabularRecordStreamReducer` wraps the per-partition consumer via a scoped `Executor` delegate. ThreadLocal remains the default; loud `NoSuchElementException` propagation on missed dispatch sites (no silent fallback). `parallelStream().map(...)` sites in `TableQueryEngine.toCuboids` are intentionally not wrapped (acknowledged leftovers pending virtual-thread migration).
- `StandardQueryOptions.PARTITIONED`: new query option that splits the output column into N hash partitions (one per available core) during aggregation. Each partition is an independent `PartitionedMergeableColumn`, reducing per-partition map size and enabling future parallel reduction across cores.
- `IConsumingStream.forEachPartitioned`: partition-aware terminal operation that routes elements to dedicated per-partition consumer threads, eliminating write contention. Used by `TabularRecordStreamReducer` when the `PARTITIONED` option is active.
- `LastLookupCache1`: new length-1 specialization of `LastLookupCache` for hot-path callers that only need a single reference key. Avoids the `Object[]` varargs allocation and list wrapping on the slow path.
- `LastLookupCache1Volatile`: alternative length-1 cache using a single `volatile` shared slot instead of a `ThreadLocal`. Eliminates the `ThreadLocal.get()` map lookup at the cost of cross-thread contention on the slot. A JMH benchmark in `jmh/.../BenchmarkLastLookupCache` compares both variants against a plain `ConcurrentHashMap` baseline.
- `LastLookupCache1Scoped`: JDK-25 `ScopedValue`-backed length-1 cache. Binds a single-field mutable `Holder` per scope (via `runInScope(Runnable)` / `callInScope(Callable)`); `ScopedValue.get()` replaces `ThreadLocal.get()` on the fast path. Silent-miss outside a scope (`getByRef` returns `null`, `slowComputeIfAbsent` skips publishing) so the cache remains safely callable from static contexts. A fourth variant added to `jmh/.../BenchmarkLastLookupCache` with scope setup amortized over 64 lookups per invocation (`@OperationsPerInvocation`).
- `SequencedSetUnsafe.internKeyset(Collection)`: new static utility that interns a column keyset into a `SequencedSetLikeList`, backed by a shared two-layer cache.
- `MultitypeNavigableIntColumn`: int-specialized companion to `MultitypeNavigableColumn` (and complement to `MultitypeHashIntColumn`). Stores ascending int keys in a primitive `IntArrayList`, exposing `append(int)` / `onValue(int)` / `appendIfOptimal(int)` without `Integer` boxing. Designed as the output column of dictionarizing aggregation layers (e.g. `AggregatingColumnsDistinct`) where indices are emitted in monotonically-ascending order, so the `onAppendLast` fast path is the common case.
- `IJooqTableSupplier` + `JooqTableWrapperParameters.tableSupplier`: opt-in hook to substitute a per-query `TableLike` in place of the constant `JooqTableWrapperParameters.table`. The `JooqTableQueryFactory` consults the supplier from its `prepareQuery(TableQueryV4)` entry point via a new `resolveTable(tableQuery)` method; `getResultForFields` and `getColumns` keep using the constant `table` for schema introspection. When `tableSupplier` is absent the behaviour is identical to today.
- `PrunedJoinsJooqTableSupplierBuilder` (renamed from `PrunedJoinsJooqSnowflakeSchemaBuilder`): subclass of `JooqTableSupplierBuilder` (renamed from `JooqSnowflakeSchemaBuilder`) that records each `leftJoin(...)` as a `JoinNode {alias, parentAlias, joinedTable, onConditions, prunable, columnsOverride}` instead of eagerly composing `snowflakeTable`. Exposes `build()` — an `IJooqTableSupplier` that, per `TableQueryV4`, (1) collects referenced columns from groupBys + aggregator column-names + filter columns (including per-aggregator `FILTER(...)` clauses), (2) resolves each column to its owning join via a `Map<columnName, joinAlias>` built from explicit `providedColumns` overrides or derived from `joinedTable.fields()`, (3) closes over parent links so every ancestor needed to reach a required child is pulled in (snowflake transitivity), (4) forces in every non-prunable node (i.e. any join registered via the low-level `leftJoinConditions`, which lacks a column contract), (5) folds the resulting alias set onto the base table in declaration order to produce the per-query `Table<Record>`. Unknown columns default to the base table — matching the existing `aliasToOriginal.putIfAbsent` convention. A bounded LRU (128 entries) caches the `referencedColumns → Set<joinAlias>` decision; the `Table<Record>` rebuild itself is not cached. `purgeColumnCache()` forces re-probing of joined-table fields on the next query. Motivation: DuckDB and similar engines pay a per-join cost in the `FROM` clause even when the join contributes no `SELECT` column.
- `JooqTableSupplierBuilder.build()` / `IJooqTableSupplier.getSchemaTable()` / `JooqTableWrapperParametersBuilder.tableSupplier(IJooqTableSupplier)`: replaces the legacy two-step `.table(schema.getSnowflakeTable()).tableSupplier(supplier)` with a single `.tableSupplier(schema.build())` call. The supplier now exposes both the per-query `tableFor(TableQueryV4)` and the all-joins `getSchemaTable()` used for schema introspection (`getColumns` / `getResultForFields`). The builder's `tableSupplier(...)` setter wires both `table` and `tableSupplier` from the supplier.

### Fixed
- `CompositeCubesTableWrapper`: a filter or groupBy on a column that is declared as `generated` / `calculated` by at least one sub-cube (e.g. a scenario hierarchy exposed by an `IDecomposition` inside a Dispatchor) is now auto-suppressed for sub-cubes that do NOT declare the column, instead of being translated to `matchNone` (filter) or a cube-name fallback coordinate (groupBy). A new helper `computeCrossCubeCalculatedColumns()` returns the set of cross-cube calculated columns; `filterForColumns` short-circuits to `MATCH_ALL` and the missing-column mask in `openStream` collapses to `IColumnGenerator.COORDINATE_GENERATED` for them. Fixes composite-cube counts when a `count(*)`-style aggregator is queried alongside a scenario hierarchy known only to some sub-cubes.
- `FilterHelpers.filterComparator()`: now correctly returns 0 for semantically equivalent filters with different operand order (e.g., `AND(a,b)` vs `AND(b,a)`). Type ranking: AND > OR > NOT > COLUMN.
- `IConsumingStream.iterator(ExecutorService)`: now calls `endOfData()` when the poison marker is read. Previously, the iterator returned `null` without transitioning Guava's `AbstractIterator` to `DONE`, causing any subsequent `hasNext()` call to block indefinitely on `queue.take()`.

### Changed
- `ColumnFilter` JSON/YAML: the `nullIfAbsent` property is now omitted from serialization when the underlying `IValueMatcher` cannot match `null` (e.g. `EqualsMatcher`, `InMatcher`, `LikeMatcher`, …), since the flag has no observable effect in that case. It is still serialized for matchers that may match `null` (e.g. `NullMatcher`, and `NotMatcher` wrapping a non-null matcher). Deserialization is unchanged: the `@Jacksonized` builder defaults `nullIfAbsent = true` when the property is absent.
- Build profiles `-Pfast` and `-Pnostyle`: now set `cyclonedx.skip=true` so the CycloneDX SBOM generation is skipped during fast recompiles and style-skipping test runs.
- `MeasureForestConcealer`: operand values inside every `ColumnFilter.valueMatcher` (`EqualsMatcher`, `InMatcher`, `NotMatcher`, `AndMatcher`, `OrMatcher`, `ComparingMatcher`, `LikeMatcher`, `StringMatcher`) are now concealed to `v_<hash>` tokens — previously only column names were hashed. Operator keys (`aggregationKey`, `combinationKey`, `editorKey`, `decompositionKey`) are concealed to `k_<hash>` unless they belong to a **per-kind** whitelist (`standardAggregationKeys`, `standardCombinationKeys`, `standardDecompositionKeys`, `standardEditorKeys`). Defaults mirror exactly what `StandardOperatorFactory.makeAggregation` / `makeCombination` / `makeDecomposition` / `makeEditor` resolves through a switch case (not the `Class.forName` fallback) — a pact validated by `TestMeasureForestConcealer#testDefaultStandardXxxKey_instantiable` which loops each whitelist through `StandardOperatorFactory`. Override via `MeasureForestConcealer.builder().standardAggregationKeys(...)` etc. to align with a custom `IOperatorFactory`. **API change**: `ConcealingDefinition.valueMapping` is now `Map<Object, String>` (was `Map<String, String>`) and gains a sibling `keyMapping: Map<String, String>`; the `ConcealingVisitor` constructor takes five extra arguments (`keyMapping` + four per-kind whitelists).
- `TabularRecordBuilder.aggregates`: switched from `ImmutableMap.Builder<String, Object>` to `PerfectHashKeyset.Builder<Object>`. `AggregatedRecordFields` now memoizes its `PerfectHashKeyset` via `Suppliers.memoize` (mirroring the existing `allColumns` pattern), so the perfect-hash table is built once per `AggregatedRecordFields` and reused by every record `TabularRecordFactory` produces. `TabularRecordOverMaps.aggregates` field type widened from `ImmutableMap<String, ?>` to `Map<String, ?>` to admit the new `PerfectHashMap`. **Behavior change**: duplicate aggregate column names are now rejected at first record build (`PerfectHashKeyset.of` throws `IllegalArgumentException("Inputs must be distinct")`) instead of per-record at `ImmutableMap.Builder.buildOrThrow` time. The exception type is unchanged; the message and timing are not.
- `GroupByColumns.retainAll`: the `RETAIN_CACHE` used to be a static `LastLookupCache<IGroupBy>(2)` keyed on `(this, retainedColumns)`. `this` is redundant when the cache is scoped to a single instance, so the cache is now a per-instance `LastLookupCache1<IGroupBy>` keyed on `retainedColumns` alone — this drops the length-2 `Object[]` allocation on the slow path and the per-element loop on the fast path. Trade-off: each `GroupByColumns` instance pays for its own small `ConcurrentHashMap` + `ThreadLocal`, but both are short-lived (query-scoped) and GC-friendly.
- `CubeWrapper.getColumns()`: aliases whose underlying column cannot be resolved on this cube are now discarded with a `WARN` log, rather than silently registered as dangling `Object.class` columns tagged `alias`. This prevents a `ColumnsManager` shared across several cubes (where some aliases are only relevant to a subset of cubes) from polluting the schema reported by each cube. The `log.debug` line also had a broken `String.format`-style `%s` placeholder, now corrected to the SLF4J `{}` form.
- `AMultitypeNavigableColumn` (new abstract parent): factored the shared write/append/scan/stream/lookup/lock/compact logic out of `MultitypeNavigableColumn` and `MultitypeNavigableIntColumn` into a common base parameterised on `T extends Comparable<T>`. Subclasses now own only the key-storage hooks (`keysSize`, `keysGet`, `keysAppendLast`, `keysInsertAt`, `keysRemoveAt`, `keysBinarySearch`, `keysEnsureCapacity`, `keysCompact`) and the optional Bloom presence pre-screen. The protected helper formerly named `merge(int)` is now `mergeAtIndex(int)` to avoid colliding with the public `IMultitypeMergeableColumn.merge(T)` signature on the int specialization. Net effect: the two navigable subclasses drop ~720 LOC of duplicated logic, the int specialization keeps its primitive `IntArrayList` storage, and the only new boxing happens at the abstract-method boundary on the slow paths (binary search / random insertion / scan).
- `AMultitypeHashColumn` (new abstract parent): identical extraction for the hash family. The new base owns the type-dispatch logic for `containsKey`, `append`, `unsafePut`, `merge`, `onValue`, `scan`, `stream`, `keyStream`, `unsafePurge` and `compact`, while subclasses provide ~24 thin abstract hooks per type-lane (size / containsKey / get / put / remove / forEach / ensureCapacity / compact). `MultitypeHashColumn` and `MultitypeHashIntColumn` shrink to just the fastutil `Object2X` / `Int2X` map declarations and one-liner forwarders. Storage stays primitive in the int variant; the per-call boxing is short-lived garbage that doesn't change the steady-state heap.
- `MultitypeNavigableIntMergeableColumn`: new int-specialized companion to `MultitypeNavigableMergeableColumn`, available for callers that want an unboxed `merge(int)` fast path. **Not** yet wired into `AggregatingColumns.makePreColumn()` because the slice-sortedness tracking that the existing `MultitypeNavigableElseHashMergeableColumn` provides for free (via its in-order/out-of-order leg dispatch) is lost without a hash-side fallback; moving that tracking into `AAggregatingColumns` is a separate task.
- `UndictionarizedColumn.column`: tightened field type from `IMultitypeColumnFastGet<Integer>` to `IMultitypeIntColumnFastGet`, so hot-path `onValue(int)` / `onValue(int, StreamStrategy)` lookups never pay `Integer.valueOf` allocation. `UndictionarizedColumn` now also implements `ICompactable`, forwarding to the inner column.
- `UndictionarizedColumn.stream(SORTED_SUB)` / `stream(SORTED_SUB_COMPLEMENT)`: switched from positional prefix/suffix (`column.limit(sortedLength)` / `column.skip(sortedLength)`) to `sortedLeg`-predicate filtering. Positional splits were only correct when the inner column's natural iteration order aligned with the slice-sorted prefix; after the refactor below, the inner column always iterates by index which may not coincide, so the filter is the authoritative path.
- `AggregatingColumnsDistinct.makePreColumn()`: now instantiates `MultitypeNavigableIntColumn` directly instead of going through `IColumnFactory.makeColumn(...)`. Indices arrive in monotonically-ascending order, so the append-last fast path is hit on every write and no boxing-through-`Integer` happens anywhere in the aggregation or close-column hot paths. `aggregatorToAggregates` is now typed `Map<String, IMultitypeIntColumnFastGet>`; the previously-unused `factories` field has been removed from the builder.
- `AggregatingColumns.undictionarizeColumn`: copies the generic `IMultitypeColumnFastGet<Integer>` produced by the non-distinct aggregation path into a fresh `MultitypeNavigableIntColumn` via `asNavigableInt(...)` before wrapping in `UndictionarizedColumn`. Columns that are already int-specialized (the `AggregatingColumnsDistinct` path) short-circuit to identity.
- `IMultitypeIntColumnFastGet.purgeAggregationCarriers()`: covariant narrowing — the interface now declares the return type as `IMultitypeIntColumnFastGet` so callers (e.g. `UndictionarizedColumn.purgeAggregationCarriers`) get the int-specialized type back without an `instanceof` re-check. `MultitypeNavigableIntColumn.purgeAggregationCarriers()` now returns the concrete class directly.
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
- `InduceByAdhocComplete`: concurrent execution of `buildEdgesForContextGroup` is now supported — when `StandardQueryOptions.CONCURRENT` is active, each measure×context group is processed in an isolated local DAG on the injected `ListeningExecutorService`, then all local DAGs are merged before the transitive-reduction step. The default executor is `MoreExecutors.newDirectExecutorService()` (sequential, as before). Inject a thread-pool via `InduceByAdhocComplete.builder().executorService(...).build()`.
- `InduceByAdhocComplete.splitInducedAsDag`: replaced O(n²) pairwise scan with three pre-computed indices — (1) grouping by measure+context to avoid cross-measure evaluation, (2) a groupBy containment index (O(g²) for g distinct groupBys), and (3) a filter strictness index (O(f²) for f distinct filters). Only `makeLeftoverFilter` is still checked per step pair, reached only when both indices agree.

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
