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
