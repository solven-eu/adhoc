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

### Ctrl+F application-level search modal

The SlickGrid view is **lazy** — only the rows currently in the viewport are
rendered to the DOM. The browser's native Ctrl+F search box therefore only
finds matches in the visible window: scroll a row off-screen and it stops
being findable, scroll back and it appears again. From the user's point of
view, "Ctrl+F can't find a value I know is in the table" is a confusing
silent miss.

We do **not** want to disable the browser's native search — for visible cells
it works fine, and stripping it would be hostile. The plan is to _augment_
it: pressing Ctrl+F triggers both the native search **and** opens a small
modal that performs an application-level search across the full result set
(not just the rendered rows). The modal must visibly catch the user's
attention so they understand "this is the search you want when the native
one comes up empty".

Implementation notes:

- Hook a `keydown` listener for Ctrl+F / Cmd+F at the grid container level.
  Do **not** call `preventDefault()` — let the browser's native find bar
  open as well.
- Modal contents: a single text input plus a result list. Each result row
  shows the matched cell's column, value, and a "scroll to" button that
  pages the grid to that row and highlights the cell briefly.
- Search scope: rows the SPA already has client-side (the materialized
  `TabularView`). For server-paged or windowed results, surface a "search on
  the server" affordance — this is where the feature meets the [coordinate
  search](#search-columns-by-coordinate-value) work above. The modal is the
  natural UX entry point for both: client-side hits first (instant), then
  optional server fan-out for deeper matches.
- Dismissal: Esc closes the modal; the native find bar's lifecycle is
  unaffected.

Open questions:

- Should the modal also match against measure values (numbers), or only
  string coordinates? Numeric matching probably needs a comparator (`>`,
  `<`, range), which is a bigger UX surface — start with strings only.
- How to highlight the navigated-to cell after "scroll to"? A short flash
  (e.g. 800ms background pulse) is the minimum; persistent highlight until
  the next search / dismiss is more discoverable but louder.

## Grid interaction

### Excel-style cell copy (Ctrl+C from a clicked cell)

Today, copying a value out of the grid is unreliable: depending on where the
user clicks and what (if anything) the browser considers selected, Ctrl+C may
copy nothing, copy a stray bit of surrounding chrome, or work only after the
user has manually drag-selected the cell's text. Users coming from Excel
expect "click a cell, Ctrl+C, paste it elsewhere" to just work — the cell's
displayed value should land in the clipboard with no extra ceremony.

Desired behaviour:

- **Click selects a cell.** A single click on a grid cell marks it as the
  active cell (a visible focus ring or border). SlickGrid's cell-selection
  model should already provide the hook — we just need to wire the visual
  affordance.
- **Ctrl+C on the active cell copies its displayed value** to the
  clipboard, even if no native text selection exists. Use the async
  Clipboard API (`navigator.clipboard.writeText`) so we are not bound to
  the document's text selection at all.
- **Honour partial text selections** when they exist. If the user has
  drag-selected a portion of the cell's text (or text spanning multiple
  cells, if SlickGrid permits it), Ctrl+C should copy _that_ selection,
  not the active cell. Detect via `window.getSelection().toString()` —
  if non-empty, fall through to the browser's default behaviour; if
  empty, take over and copy the active cell.
- **Multi-cell selection** (later): rectangular range selection with the
  mouse, then Ctrl+C produces a TSV blob (Excel-pasteable). Out of scope
  for the first iteration; the single-cell case is the high-value win.

Open questions:

- Which value to copy: the _displayed_ string (post-formatter — e.g.
  `1,234.50` for a number), or the _raw_ underlying value (`1234.5`)? The
  Excel mental model says "what I see is what I copy", so default to
  displayed; offer Ctrl+Shift+C for the raw value if the need arises.
- Does the copy-on-empty-selection rule conflict with the [Ctrl+F app
  modal](#ctrlf-application-level-search-modal) plan? No — different
  shortcut, different state, but worth keeping the two specs reviewed
  together so we don't accidentally swallow each other's keystrokes.
- Visual feedback: a brief toast / cell-flash on copy makes "did anything
  happen?" answerable without leaving the grid.

### Augment right-click with cell actions (filter, drillthrough, …)

The cell-level actions Pivotable already exposes — filter on this cell's
coordinate, drillthrough this cell's slice, etc. — currently live behind a
**double-click modal**. Discoverability is poor: users don't know the modal
exists until somebody points them at it, and even after they know, the
double-click → modal → click round-trip is heavier than it needs to be for
"give me the rows behind this cell".

The native right-click context menu is the conventional home for "what can
I do with this thing?". Browsers ship a default menu (Copy, Inspect, …) that
we should not strip out — power users rely on it. Augment, don't replace:
add a small Pivotable-branded section with the cell-context actions on top
of (or alongside) the browser default.

Two implementation routes:

- **Custom menu, native fallback.** Suppress the browser default with
  `event.preventDefault()` on `contextmenu` and render our own menu that
  _includes_ the relevant browser actions plus the Pivotable ones.
  Pro: full control of layout, keyboard support, theming. Con: re-implementing
  Copy / Paste / Inspect / Save image is a rabbit hole — and we'll never
  match every browser's full default menu.
- **Browser-default + side panel.** Let the browser show its own menu, and
  surface the Pivotable actions through a sticky in-grid affordance (a small
  toolbar that fades in over the active cell, or a left-rail panel that
  updates with the active cell). Pro: zero conflict with the OS / browser.
  Con: less discoverable than a right-click menu — defeats the goal.

Recommendation: start with route 1 (custom menu via `preventDefault`) but
keep the menu **short and Pivotable-specific** — Filter on this cell,
Drillthrough this slice, Copy value (folds in the [Ctrl+C
work](#excel-style-cell-copy-ctrlc-from-a-clicked-cell)) — and add an
"Open browser menu" entry that re-fires a synthetic `contextmenu` without
our handler if a user really needs the browser default. Most users will
never need it.

Open questions:

- Which actions belong in the right-click vs. only in the (existing)
  double-click modal? Likely: right-click = the 2–3 most common actions
  the user wanted _now_; modal = the long tail (column metadata, share,
  configure formatter, etc.). Keep them in sync — adding an action to the
  modal is the moment to ask "does this also belong in right-click?".
- Coordinate selection model: when a user right-clicks a cell, is the
  active cell that cell, or whatever they had selected before? Excel and
  most spreadsheets switch the active cell on right-click — match that.
- Mobile / touch: long-press is the conventional analogue. SlickGrid's
  touch story is its own roadmap item; flag this as a dependency rather
  than blocking on it.

