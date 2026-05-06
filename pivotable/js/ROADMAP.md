# Pivotable JS — Roadmap / Known issues

Pending UX / feature work that is known but not yet scheduled. Items here are _not_
a commitment; they are a queue, ordered loosely by user-visible value. Move an item
to `CHANGES.MD` under `### Added` / `### Fixed` once it ships.

## Error surfacing

### Grid error tooltip scrolls off-screen

When a per-cell or per-measure error tooltip opens below the fold (e.g. on a small
viewport or after scrolling the grid), it is clipped / invisible because it lives
in the grid's scrollable container instead of a body-level portal.

Scope: investigate whether SlickGrid exposes a `tooltipRoot` / `appendTo` hook,
or wrap the tooltip in a teleport/portal that renders on `document.body` with
`position: fixed`. A sticky "query is broken" banner (shipped 2026-04-22) already
handles the _query-level_ failure case; this roadmap item is specifically for
_cell-level_ or _measure-level_ tooltips.

## Query history

### Snapshot-based back/forward (done)

Browser back/forward now restores the queryModel from the URL hash — shipped
2026-04-22. See `CHANGES.MD` for the detailed behaviour.

Known limitation noted inline in `adhoc-query.js`: back/forward re-triggers a full
query recomputation (no cached `TabularView` restoration).

### Persistent, navigable query history

Today the only per-session history is the browser back/forward stack, which is
linear and opaque. It is easy to get lost after a few branching edits.

Desired feature: a dedicated "query history" panel, showing previous queryModels
the user has run, with:

- A **tree** view so branches are visible when the user backtracks and edits again
  from a prior state (the browser back/forward flattens this to a line and loses
  the branching).
- A **diff** per node highlighting what changed vs. its parent (added/removed
  measure, added/removed column, filter edit, option toggle).
- One-click "jump back" to any node — same mechanic as the "restore last
  successful query" button already in the error banner.

Open questions:

- Where to persist history? In-memory is the simplest but drops on refresh. Pinia
  with `localStorage` plugin is probably the right default; per-cube may want a
  separate key so histories don't bleed across cubes.
- How to trim? A few dozen nodes are fine; thousands will bloat localStorage. LRU
  on node count, or prune branches that were never explored deeper than N.
- Naming: "history" suggests linear, "exploration tree" is more accurate; pick
  one term and use it consistently in the UI.

### Cached TabularView restoration

Pair with the history feature above: when jumping back to a node that was run
before, we could restore the pre-computed `TabularView` from an in-memory cache
keyed by the query hash, skipping the backend round-trip entirely. Tradeoff: the
cached data may be stale if the underlying dataset has changed since, so surface
a "reload" affordance and default to "stale is fine for navigation" on small
cubes. Large cubes may want a cache TTL.

## Column discovery

### Search columns by coordinate value

Today the only column-discovery affordance is the recent "search across all
columns at once" feature (a single text query that the backend resolves into a
union of `getCoordinate` calls). Users still cannot ask "which column has a
coordinate equal to / matching `<value>`" — they must guess which column to
groupBy on, then scroll its coordinates.

Desired feature: a search box that takes a coordinate value (and optionally a
matcher: `==`, `like`, `regex`) and returns the list of `{column, coordinate}`
hits. Implementation-wise it relies on the existing `getCoordinate` endpoint
but with a per-column filter on the requested value, fanned out across columns.

This is a powerful feature but **CPU-expensive** on the backend — every column
becomes a candidate and the search has no natural prefilter. Two-phase rollout
to keep the cost manageable:

1. **Phase 1 — local cache of known matches.** As the user groups-by columns
   over a session, Pivotable already learns each column's coordinate set.
   Cache it client-side (Pinia + `localStorage`, keyed by cube + column).
   When the user types a value, first answer from the cache: "you previously
   saw `<value>` in column `<X>`, restore the groupBy on `<X>`". This is the
   restore-previously-seen-groupBy variant — zero backend cost, instant.
2. **Phase 2 — opt-in cross-column search.** A secondary action (e.g. a
   "search everywhere" button shown alongside the cached hits) triggers the
   full backend fan-out. Surface the cost in the UI ("this scans N columns,
   may take a few seconds") so the user opts in deliberately. Server-side
   may want a cap on N or a cancellation hook.

Open questions:

- Cache key: just `(cube, column)` or also a coordinate-set hash? The latter
  invalidates correctly when the cube's data changes; the former is simpler
  but stale.
- Matcher choice: start with `==` only? `like` and `regex` multiply backend
  cost meaningfully — defer until phase 2 lands.
- UX placement: integrate into the existing cross-column search bar with a
  "match coordinates" toggle, or a dedicated "find a value" entry point?
