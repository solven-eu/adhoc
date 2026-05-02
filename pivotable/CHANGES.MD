This document is intended for Pivotable developers and users.

It collects entries that affect the Pivotable backend (`pivotable/server-{webflux,webmvc}`), the SPA (`pivotable/js`), or Pivotable-specific configuration. Entries that affect the core Adhoc query engine and shared libraries live in the root [CHANGES.MD](../CHANGES.MD).

We adhere to the [keepachangelog](https://keepachangelog.com/en/1.0.0/) format.


## [0.1.0]

Bump to 0.1.0 to mark the move to JDK 25 (still 0.x — Pivotable is not yet API-stable).

## [0.0.19]

### Added
- Query grid (`adhoc-query-grid.js`): empty-state hint shown when the grid has no rows and nothing is loading — an arrow-left icon and the text "Use the wizard to pick columns and measures, then Submit to build a query.", pointing the user to the wizard panel on the left.
- Query grid measure columns: heatmap rendering. Numeric cells get a background color on a diverging red → transparent → green scale based on the measure's observed `[min, max]` range, capped at alpha 0.5 so the digits stay readable. The footer row now surfaces `min · sum · max` per measure (reusing the column's `Intl.NumberFormat`), replacing the previous placeholder. Pure-function helpers live in the new `adhoc-query-grid-heatmap.js` and are unit-tested in `unit-tests/query-grid-heatmap.spec.js`.
- Importmap extracted from `index.html` into four static JSON files — `ui/importmap-{webjars,cdn}{,-min}.json` — one per (cdn vs webjars) × (full vs minified) combination. The bootstrap block in `<head>` reads `?cdn` and `?dev` query params and emits a `<script type="importmap" src="…">` via `document.write`, so the browser applies the map before module-script execution. **Default = minified webjars**; `?dev` flips to full / source-readable builds for DevTools work. Requires Chrome 127+ / Firefox 139+ / recent Safari. Tests in `unit-tests/index-html-default-mode.spec.js` enforce key parity and version alignment across all four files. The CDN `lodashEs` entry now uses jsdelivr's single-file `/npm/lodash-es@4.17.21/+esm` build — one request instead of ~300 ESM sub-modules.
- Preferences modal (`adhoc-preferences-modal.js`): new navbar gear button opens a dialog with toggles for `?cdn` / `?dev` (apply + reload) and a Bootstrap `data-bs-theme` picker (light / dark / auto, persisted in `localStorage`).
- Navbar "expires in …" countdown is now a clickable button that manually refreshes the access token via `forceLoadUserTokens`, with an in-flight `spinner-border-sm` and a disabled state so a rapid double-click cannot fire two overlapping refreshes.
- Performance strip under the grid now updates live while a query is running: each step writes a `<stage>_startedAt` timestamp at start and clears it at end, the bar ticks at 100 ms while any stage is active, and the active stage is rendered in primary colour with an ellipsis (`executing=345ms…`). The interval is torn down as soon as every stage has a final duration.
- Measure DAG modal (`adhoc-measures-dag.js`): centred spinner + "Loading measure graph…" message while Mermaid renders for the first time. Mermaid 11+ lazy-loads its flowchart bundle on first render, which previously left the modal blank for several seconds; the spinner only shows when no SVG has been produced yet so it does not flicker on subsequent re-renders.
- Navbar countdown pill: when the access token is past its expiry, the copy switches to "expired since X" and the pill renders in `text-danger` with a warning-triangle icon instead of the muted clock icon — making it visually obvious the user is in stale-token territory and the click action is corrective.
- `simple` cube (`InjectSimpleExampleCubesConfig`): new `always_throws` measure wired on top of `delta` via `ThrowingCombination.class.getName()`. Used by Pivotable UI tests to check how a failing measure surfaces in the query grid / navbar / server logs.

### Changed
- `PivotableSpaRouter` / `PivotableSpaController`: removed server-side `minifyHtml` / `filterIndexHtml`. Minified-vs-full asset selection is now fully client-side via `?min`. The `prdmode` Spring profile no longer triggers any HTML rewriting — operators wanting minified bundles in production must include `?min` in the SPA URL.
- Query-raw modal (`adhoc-query-raw-modal.js`): new **SQL** tab next to the existing JSON / MDX tabs, rendering a minimal SQL projection of the current queryModel via a new `adhoc-query-to-sql.js` module (selected columns → `SELECT` + `GROUP BY`; selected measures → bare `SELECT` identifiers; column-equality filters → `WHERE`; `withStarColumns` → `ROLLUP(col)`; unknown matchers → `/* … */` comment, same contract as the MDX converter). Both the MDX and the SQL tab now carry a prominent **"Informative only"** `alert alert-info` banner stating that Adhoc does not execute MDX or SQL — the derived text is there to help the user hand-translate the current query into a real MDX-native (Excel, Mondrian, Atoti, …) or SQL-native (DuckDB, Postgres, BigQuery, …) client. Covered by new `unit-tests/query-to-sql.spec.js` (21 cases: empty model, measures-only, columns-only, mixed, AND/OR/NOT filters, `equals` matcher object form, unknown matcher fallback, disabled-filter stripping, numeric/boolean literals, single-quote escaping in literals, double-quote escaping in identifiers, `withStarColumns` → `ROLLUP`, empty cube name).
- `PivotableTokenService`: access-token and refresh-token lifetimes are now configurable via `adhoc.pivotable.oauth2.access-token.validity` and `adhoc.pivotable.oauth2.refresh-token.validity` (ISO-8601 durations). Defaults unchanged (`PT1H` / `P365D`). New test-only profile `application-pivotable-e2e-shorttoken.yml` sets them to `PT3S` / `PT30S` so e2e tests can exercise the real token-refresh and re-login paths in seconds instead of hours.
- `GET /api/login/v1/json`: when the caller is logged-in, the response is enriched with a `session` sub-map exposing the sliding-idle lifetime (`maxIdleSeconds`, `creationEpochMs`, `lastAccessEpochMs`). Lets the SPA render a session-expiry countdown without needing to read the HttpOnly `SESSION`/`JSESSIONID` cookie. Implemented in both webflux (`PivotableLoginWebfluxController`) and webmvc (`PivotableLoginWebmvcController`). Anonymous callers still get the previous `{ "login": 401 }` shape — no session is force-created by the probe.
- `docs/pivotable-security.md`: new article describing the SPA authentication model — identity layer (BASIC/OAuth2 → `SESSION` cookie) vs API layer (applicative JWT `access_token`), their lifetimes, and the `/api/login/v1/*` route map. Includes a Mermaid sequence diagram of the full login flow.
- Query wizard filter tree (`adhoc-query-wizard-filter.js`): an empty root filter (`queryModel.filter === {}`, i.e. the "no filter applied" state) no longer renders as the literal `{}` via the generic `{{filter}}` fallback. It now renders as a muted secondary pill with a `bi-infinity` icon and the label **matchAll**, with a tooltip "No filter — matches all rows (matchAll)". Terminology follows the existing JS convention (`adhoc-query-executor.js:107`, `adhoc-query-wizard-column.js:91`) and aligns with the backend's `IAdhocFilter.MATCH_ALL`.
- Column filter modal (`adhoc-query-wizard-column-filter.js`): the equals / not_equals editor is now a single filterable combobox instead of a raw `<input>` plus a disconnected `<select>` of coordinates. Users type freely, and a dropdown of matching coordinates (case-insensitive substring, capped at 50) appears below the input with keyboard navigation (ArrowUp/Down, Enter to pick, Escape to dismiss) and mouse-click selection. The like / JSON editors pick up the same Bootstrap form-control sizing, and a small "Unsaved changes" indicator replaces the earlier `pending={{pendingChanges}}` debug text.
- `pivotable-unsafe` profile: `server.forward-headers-strategy: framework` enabled so Spring Security / OAuth2 build redirects against the reverse-proxy's external host (Vite dev server on `:5173`) instead of the Netty backend's own `:8080`. Paired with a shared `backendProxy` in `pivotable/js/vite.config.js` (`changeOrigin: true`, `xfwd: true`, plus a custom `proxyReq` handler injecting `X-Forwarded-Host` — node-http-proxy's `xfwd` covers For/Port/Proto but not Host). Scoped to `pivotable-unsafe` because in production Spring Boot serves the SPA itself and no proxy is involved.
- `pivotable/js/package.json`: added `dev_stack` (runs `npm run backend` + `npm run dev_frontend` in one subshell, `trap 'kill 0' SIGINT` so Ctrl-C terminates both), renamed the previous `dev` script to `dev_frontend` to make the frontend/full-stack distinction explicit. `playwright.config.mjs` `webServer.command` now delegates to `npm run backend` so the backend invocation lives in one place and honours `$WEBMODE` + `$SPRING_ACTIVE_PROFILES`. Documented under "Local dev stack (Pivotable)" in `CLAUDE.md`.
- Query view Submit block: when any wizard accordion (columns / measures / custom markers / options) is expanded, the Submit button now floats over the top of the grid (`position: fixed`) so it stays visible without the user scrolling past a tall accordion body. Collapsing the accordion re-docks the button to its normal position below the wizard. Clicking Submit programmatically collapses the open accordion via Bootstrap's `Collapse.hide()`, so a round-trip query re-docks the button automatically. Implementation: shared reactive `accordionState` provided by `adhoc-query.js`, maintained via `document`-level listeners on `show.bs.collapse` / `hidden.bs.collapse` (Bootstrap 5 native events bubble).

### Fixed
- Column filter modal singleton (`adhoc-query-wizard-column-filter-modal-singleton.js` / `adhoc-query-wizard-column-filter.js`): opening the filter modal on a different column from the grid header used to pre-fill the freetext input with the value last typed for the previous column. Root cause: the grid-header modal mounts ONE child `AdhocQueryWizardColumnFilter` and swaps its `:column` prop across clicks, so internal `ref("")` state (`equalsValue`, `filterType`, `likePattern`…) survived across columns. Fix: `watch(() => props.column, …, { immediate: true })` resets local state on every column change and pre-loads any already-saved filter for the new column from `queryModel.filter`, so reopening edits the current filter instead of starting blank. Per-column modals (wizard sidebar) already mounted one instance per column — the reset is harmless there and guards against any future refactor. New e2e spec `localhost8080-column-filter-reset.spec.js` asserts the reset end-to-end.
- Favorites modal (`adhoc-query-favorite.js`): the name/path edit rows both used to label themselves "name =", so the path row displayed "name =" followed by the path value. Relabeled the second row to "path =". Drive-by: added e2e spec `localhost8080-query-favorite-persist.spec.js` covering the full round-trip — save a favorite, clean-reload (F5 equivalent), reopen from the favorites list, grid restored.
- Query view "Restore last successful query" button: the banner now also updates the WIZARD (left sidebar switches) to reflect the restored queryModel — previously `always_throws` (or whichever measure caused the failure) stayed visually checked even though `selectedMeasures` had dropped it logically. Root cause: `queryModel.reset()` and `parsedJsonToQueryModel` reassigned fresh `{}` objects to `selectedMeasures` / `selectedColumns` / `withStarColumns` / `filter` / `customMarkers` / `selectedOptions`, breaking the per-key reactive chain for `v-model` bindings that had already resolved to the prior proxy. Fix: clear in-place (`delete key`) in `reset`, and use `Object.assign` in `parsedJsonToQueryModel` rather than reassigning the parent reference. New e2e spec `localhost8080-query-restore-last-successful.spec.js` asserts the switches reflect the restored state.
- Query view "Reset query" after a failure: clicking `Reset query` while the "query is broken" banner is up now lands on a clean empty view (banner gone, grid empty, wizard cleared). Three-part fix: (a) `queryModel.reset()` now uses `this` instead of its closure-captured raw reference — the closure pointed at the pre-`reactive()` target, so mutations never fired Vue effects and the UI stayed frozen despite the model being logically cleared; (b) the executor's autoQuery watcher short-circuits when both `measures` and `groupBy.columns` are empty, clearing `tabularView.view` / `tabularView.error` instead of firing an empty-query round-trip that would re-populate the banner; (c) the grid's `resyncData` renders an empty grid when `tabularView.view` is null, rather than dereferencing undefined `.coordinates`. New e2e spec `localhost8080-query-reset-after-failure.spec.js` covers the flow end-to-end on the `simple` cube using the `always_throws` measure.
- Query view: a failing query now surfaces a prominent sticky banner above the grid (previously the only indication was a small alert badge next to the Submit button, easy to miss when scrolling). The grid continues to show the last successful result so the user keeps context; the banner offers a "Restore last successful query" button that rolls the `queryModel` back to the snapshot captured when the most recent `TabularView` was received. Faulty measures / columns / filters that caused the failure are dropped cleanly because `parsedJsonToQueryModel` does a full reset before populating. Implementation: `tabularView.error` carries the error message from `adhoc-query-executor.js`; `lastSuccessfulQuery` (a plain JSON snapshot via `queryModelToParsedJson`) lives on `adhoc-query.js` and is refreshed on every `tabularView.view` assignment.
- New `pivotable/js/ROADMAP.md` collects known UX gaps and feature ideas: off-screen grid tooltip clipping, persistent navigable query history with diff / tree view, cached `TabularView` restoration on back-navigation.
- Query view: browser back/forward now restores the previous query model (groupBy, measures, filter, customMarkers, options) instead of silently changing the URL while the UI kept the current state. Two fixes: (a) a new watch in `adhoc-query.js` listens to `router.currentRoute.value.hash` and decodes it into `queryModel` on popstate, using a hash-equality guard to break the feedback loop with the queryModel→pushState watcher; (b) `adhoc-query-helper.js:parsedJsonToQueryModel` now calls `queryModel.reset()` before populating, so decoding the hash acts as a full snapshot replacement (previously columns/measures added after the restored state would stick, defeating back/forward). Unit test `query-hash.spec.js#loadQueryModelFromHash - replaces prior state` covers the replacement semantics. Known limitation noted inline as a TODO: back/forward re-triggers a full query recomputation — no cached TabularView restoration.
- `login-providers.js`: selecting the BASIC provider from inside the loginModal now renders the username/password form in-place instead of triggering a full-page navigation to `/html/login/basic`. The modal branch was gated on `item.registration_id == 'BASIC'`, but the backend returns `registration_id = 'pivotable-unsafe_fakeuser'` (the profile name), so the gate never matched and the template fell through to `<a :href="item.login_url">`. Gate now uses `item.type == 'basic'`. New e2e spec `localhost8080-login-modal-basic.spec.js` regression-tests the scenario.

## [0.0.17]

### Added
- `pivotable-mcp` module: exposes Pivotable as an MCP server (list endpoints, browse schemas, execute queries) via `PivotableMcpTools`.
- `PivotableChatHandler`: optional AI query assistant in the SPA, streaming tool calls to update the query model in real time (requires `adhoc.pivotable.chat.anthropic-api-key`).

## [0.0.15]

### Changed
- Pivotable spring profiles has been reworked. There is more a default `application.yml`, but one should import `pivotable-config.yml`.

## [0.0.13]

### Added
- Pivotable will automatically cancel currently running queries if another query is triggered.

### Fixed
- VueJS router is properly waited for (with `.isReady()`) to load the queryModel from `#hash` on relogging.

## [0.0.12]

### Fixed
- Actuator configuration in Pivotable has been reviewed. `/actuator/info` and `/actuator/health` are public.

## [0.0.10]

### Added
- Playwright is activated to integration-tests Pivotable.

## [0.0.9]

### Added
- Additional `WorldCup` example cube, with more complex measures.
- In Pivotable, tag filtering is now cross-columns, in addition to cross-measures.
- In Pivotable, clicking a column header will sort rows based on given column.

## [0.0.6]

### Added
- Enable Export to CSV in `Pivotable`
- Enable Export to Clipboard in `Pivotable`

### Changed
- In `Pivotbale`, `customMarkers` are written on a per-value basis, with the `path` as key.

## [0.0.5]

### Added
- Enable `tags` measure filtering in `Pivotable`

### Changed
- `Pivotable` opens a filter-helper when double-cliking a cell of the grid.
- `Pivotable` search now highlights/marks matching elements.

## [0.0.3]

### Added
- `Pivotable` can now drop column/measure from grid headers.
- Fix serialization of `IQueryOptions`, and update `Pivotable` for proper options.
