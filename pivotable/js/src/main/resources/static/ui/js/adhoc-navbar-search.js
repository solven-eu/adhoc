// @ts-check
import { ref, computed, watch } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";
import { useSearchStore } from "./store-search.js";
import { searchTabularView } from "./adhoc-search-helper.js";
import { searchCoordinatesAcrossColumns } from "./adhoc-coordinate-search-helper.js";
import { searchCubeSchema } from "./adhoc-schema-search-helper.js";

// Compact search input in the navbar middle. Searches the currently-rendered query grid's
// materialized rows for a substring (case-insensitive, matches both the raw String(value)
// and the formatter's output). Results render in a dropdown beneath the input; clicking a
// hit scrolls SlickGrid to that row and flashes the cell.
//
// Explicitly does NOT intercept Ctrl+F — the browser's native find bar continues to work
// for visible cells; this search complements it by reaching rows the lazy grid hasn't
// rendered yet.
//
// When no grid is active (user is on /html/endpoints or similar), the dropdown shows a
// gentle "no active grid" hint so the affordance is discoverable without being broken.
export default {
	computed: {
		// `columns` is the per-(endpoint, cube, column) coordinate-sample cache populated by
		// `loadColumnCoordinates` and the bulk-loader. The coordinate-search-across-columns
		// path consults it directly — no separate cache needed.
		...mapState(useAdhocStore, ["columns"]),
	},
	setup() {
		const searchStore = useSearchStore();

		const query = ref("");
		// Debounce the search so a long view doesn't get scanned on every keystroke. 80ms is
		// short enough to feel instant, long enough to absorb a fast typist's keydown burst.
		const debouncedQuery = ref("");
		/** @type {ReturnType<typeof setTimeout> | null} */
		let debounceHandle = null;
		watch(query, (next) => {
			if (debounceHandle) clearTimeout(debounceHandle);
			debounceHandle = setTimeout(() => {
				debouncedQuery.value = next;
			}, 80);
		});

		const activeGrid = computed(() => searchStore.activeGrid);
		const dropdownOpen = ref(false);

		// Hit list — recomputed reactively whenever the active grid swaps, the query changes,
		// or (less common) the view object itself is replaced by a new query result.
		const hits = computed(() => {
			const ctx = activeGrid.value;
			const needle = debouncedQuery.value;
			if (!ctx || !needle || !needle.trim()) return [];
			return searchTabularView(
				{
					view: ctx.view,
					coordinateColumns: ctx.coordinateColumns,
					measureColumns: ctx.measureColumns,
					formatCell: ctx.formatCell,
					limit: 100,
				},
				needle,
			);
		});

		// Expand/collapse state. The bar renders as a compact ~10rem input by default — wide
		// enough to surface the search icon + placeholder without dominating the navbar — and
		// expands to ~28rem on focus so the user has room to type a longer query. Same idiom
		// as GitHub's navbar search. Width animates via a CSS transition on `max-width`.
		// Stays expanded while there is a query (so the dropdown's results aren't squeezed
		// after the input blurs from a click on a hit), collapses back when blurred AND
		// empty.
		const expanded = ref(false);
		const onFocus = () => {
			expanded.value = true;
			dropdownOpen.value = true;
		};
		const onBlur = () => {
			// Defer the close so a click on a hit (mousedown → blur → click) still lands on the
			// hit's button. 150ms is a Bootstrap-typical hover-out grace window.
			setTimeout(() => {
				dropdownOpen.value = false;
				if (!query.value) {
					expanded.value = false;
				}
			}, 150);
		};

		// Index of the most recently-jumped hit. Drives the Enter / Shift+Enter cycling and
		// the "match X of Y" pill in the dropdown header. Resets to -1 whenever the query
		// changes so the next Enter lands on the first match of the new search.
		const currentHitIndex = ref(-1);
		watch(debouncedQuery, () => {
			currentHitIndex.value = -1;
		});

		const jumpToHit = (index) => {
			const ctx = activeGrid.value;
			if (!ctx || typeof ctx.scrollToRow !== "function") return;
			const list = hits.value;
			if (!list || list.length === 0) return;
			const wrapped = ((index % list.length) + list.length) % list.length;
			currentHitIndex.value = wrapped;
			const hit = list[wrapped];
			try {
				ctx.scrollToRow(hit.row, hit.column);
			} catch (e) {
				console.warn("scrollToRow failed", e);
			}
		};

		const onHitClick = (hit, index) => {
			jumpToHit(index);
		};

		// Phase B1: coordinate search across columns. Consults the SPA's session-grown
		// column-coordinate cache (`state.columns`) so the SAME helper that powers the
		// wizard's filter dropdowns also powers a "found in column <X>" hint. No backend
		// round-trip; instant. The hits are scoped to the active grid's (endpoint, cube) and
		// exclude columns already in the groupBy — those would produce a no-op click.
		const adhocStore = useAdhocStore();
		const coordinateHits = computed(() => {
			const ctx = activeGrid.value;
			const needle = debouncedQuery.value;
			if (!ctx || !needle || !needle.trim()) return [];
			const alreadyInGroupBy = ctx.queryModel
				? Object.keys(ctx.queryModel.selectedColumns || {}).filter((k) => ctx.queryModel.selectedColumns[k] === true)
				: [];
			return searchCoordinatesAcrossColumns({
				columns: adhocStore.columns,
				endpointId: ctx.endpointId,
				cubeId: ctx.cubeId,
				query: needle,
				excludeColumns: alreadyInGroupBy,
				limit: 50,
			});
		});

		// Click handler for a coordinate hit. Adds the matching column to the active grid's
		// queryModel groupBy (per ROADMAP "restore the groupBy on <X>") — lossless: the user
		// gets the column in their wizard and can filter on the coordinate themselves as a
		// deliberate second step. We do NOT auto-apply the filter — keeping the action
		// reversible matches the ROADMAP framing and avoids surprising the user with
		// query-shape changes they didn't ask for.
		const onCoordinateHitClick = (hit) => {
			const ctx = activeGrid.value;
			if (!ctx || !ctx.queryModel) return;
			const qm = ctx.queryModel;
			if (!qm.selectedColumns) return;
			qm.selectedColumns[hit.column] = true;
			if (typeof qm.onColumnToggled === "function") {
				qm.onColumnToggled(hit.column);
			}
			// Leave the dropdown open so the user can keep exploring matches on other columns,
			// but reset the cycle-counter so a subsequent Enter on row-hits starts fresh.
			currentHitIndex.value = -1;
		};

		// Phase B1.b: schema search — match column NAMES and measure NAMES from the cube
		// itself (not coordinates). Surfaces "the column you might be looking for is X" so
		// the user doesn't have to scroll the wizard to find and tick it. Source data is the
		// already-loaded `schema.cubes[cubeId]` carried by the active grid context (via the
		// adhoc store's `schemas` map).
		const cubeSchema = computed(() => {
			const ctx = activeGrid.value;
			if (!ctx || !ctx.endpointId || !ctx.cubeId) return null;
			const endpointSchema = adhocStore.schemas[ctx.endpointId];
			if (!endpointSchema || !endpointSchema.cubes) return null;
			return endpointSchema.cubes[ctx.cubeId] || null;
		});
		const schemaHits = computed(() => {
			const ctx = activeGrid.value;
			const cube = cubeSchema.value;
			const needle = debouncedQuery.value;
			if (!ctx || !cube || !needle || !needle.trim()) return [];
			return searchCubeSchema({
				columns: (cube.columns && cube.columns.columns) || {},
				measures: cube.measures || {},
				selectedColumns: ctx.queryModel ? ctx.queryModel.selectedColumns : {},
				selectedMeasures: ctx.queryModel ? ctx.queryModel.selectedMeasures : {},
				query: needle,
				limit: 30,
			});
		});

		// Click on a schema hit: tick the corresponding column / measure in the queryModel.
		// `onColumnToggled` is required for columns (it maintains `selectedColumnsOrdered`);
		// measures only need the boolean flag. Re-clicking an already-selected one is treated
		// as a no-op — the visible "already added" hint should prevent the duplicate, but if
		// the user manages to fire the click anyway we don't want to flip-flop the state.
		const onSchemaHitClick = (hit) => {
			const ctx = activeGrid.value;
			if (!ctx || !ctx.queryModel) return;
			const qm = ctx.queryModel;
			if (hit.kind === "column") {
				if (!qm.selectedColumns) return;
				if (qm.selectedColumns[hit.name] === true) return;
				qm.selectedColumns[hit.name] = true;
				if (typeof qm.onColumnToggled === "function") {
					qm.onColumnToggled(hit.name);
				}
			} else if (hit.kind === "measure") {
				if (!qm.selectedMeasures) return;
				if (qm.selectedMeasures[hit.name] === true) return;
				qm.selectedMeasures[hit.name] = true;
			}
			currentHitIndex.value = -1;
		};

		// Phase B2 — opt-in backend fan-out. The Phase B1 coordinate search only knows what the
		// SPA has previously loaded; if the user types a value that lives in a column they've
		// never opened, the cache misses. Clicking "Search across all columns…" triggers
		// `loadColumnCoordinatesIfMissing` for every column of the cube. Each call stores its
		// result into `state.columns[<id>].coordinates`; the Phase B1 reactivity then surfaces
		// the new matches automatically. Sequential progress lets the user watch the work
		// happen and gives the dropdown a natural "fan-out is running" state.
		//
		// IMPLEMENTATION NOTE: this used to fan out per column with N parallel calls. The
		// backend's `/endpoints/schemas/columns?cube=X` (no `name` filter) ALREADY answers
		// every column in a single HTTP round-trip — server-side it calls the bulk
		// `ICubeWrapper.getCoordinates(Map, int)` which lets the engine share scans across
		// columns (`SELECT COUNT(DISTINCT col1), COUNT(DISTINCT col2), …` on SQL). So one
		// `loadAllCubeColumnsCoordinates` call is strictly better than N per-column ones.
		const fanOutState = ref(
			/** @type {{ phase: "idle"|"running"|"done", columnsLoaded: number }} */ ({
				phase: "idle",
				columnsLoaded: 0,
			}),
		);

		// Columns that haven't yet been coordinate-loaded. Drives the button's visibility —
		// once everything is cached the affordance hides since there's no work to do.
		const unloadedColumns = computed(() => {
			const ctx = activeGrid.value;
			const cube = cubeSchema.value;
			if (!ctx || !cube || !cube.columns || !cube.columns.columns) return [];
			const allColumnNames = Object.keys(cube.columns.columns);
			return allColumnNames.filter((name) => {
				const id = `${ctx.endpointId}-${ctx.cubeId}-${name}`;
				const entry = adhocStore.columns[id];
				return !entry || !Array.isArray(entry.coordinates);
			});
		});

		const runFanOut = async () => {
			const ctx = activeGrid.value;
			if (!ctx) return;
			const toLoadCount = unloadedColumns.value.length;
			if (toLoadCount === 0) return;
			fanOutState.value = { phase: "running", columnsLoaded: toLoadCount };
			try {
				// One round-trip — the server iterates every column and returns each one's
				// coordinates sample via the bulk getCoordinates(Map, int) path. Writes into
				// `state.columns[<endpointId>-<cubeId>-<column>]`; Phase B1's reactive
				// `coordinateHits` then surfaces any matches automatically.
				await adhocStore.loadAllCubeColumnsCoordinates(ctx.cubeId, ctx.endpointId);
			} catch (e) {
				console.warn("fan-out: bulk loadAllCubeColumnsCoordinates failed", e);
			}
			fanOutState.value = { phase: "done", columnsLoaded: toLoadCount };
		};

		// A new query should let the user kick off another fan-out (different value → different
		// columns may be relevant). Reset to idle whenever the debounced query changes —
		// preserves the "running" state across query edits so the in-flight bulk call's
		// progress isn't visually clobbered by typing.
		watch(debouncedQuery, () => {
			if (fanOutState.value.phase !== "running") {
				fanOutState.value = { phase: "idle", columnsLoaded: 0 };
			}
		});

		// Enter cycles forward through hits; Shift+Enter cycles backward — same idiom as the
		// browser's native find bar. Both wrap around the end of the list. preventDefault on
		// Enter is fine here because the input is NOT inside a <form> (no implicit submit to
		// suppress); the modifier short-circuits any text-input default action.
		//
		// Escape dismisses the dropdown without erasing the query — same behaviour as the
		// browser's native find bar (Escape closes the bar but the page state stays as-is).
		// Also blurs the input so the navbar reclaims its compact form and a follow-up Tab
		// goes to the next focusable element rather than re-opening the dropdown.
		const onKeydown = (event) => {
			if (event.key === "Enter") {
				event.preventDefault();
				const step = event.shiftKey ? -1 : 1;
				const next = currentHitIndex.value < 0 ? (step > 0 ? 0 : -1) : currentHitIndex.value + step;
				jumpToHit(next);
			} else if (event.key === "Escape") {
				event.preventDefault();
				dropdownOpen.value = false;
				if (event.target && typeof (/** @type {any} */ (event.target).blur) === "function") {
					/** @type {any} */ (event.target).blur();
				}
			}
		};

		const clearQuery = () => {
			query.value = "";
			debouncedQuery.value = "";
		};

		return {
			query,
			debouncedQuery,
			hits,
			coordinateHits,
			schemaHits,
			activeGrid,
			dropdownOpen,
			expanded,
			currentHitIndex,
			fanOutState,
			unloadedColumns,
			runFanOut,
			onFocus,
			onBlur,
			onHitClick,
			onCoordinateHitClick,
			onSchemaHitClick,
			onKeydown,
			clearQuery,
		};
	},
	template: /* HTML */ `
		<div class="position-relative" :style="'max-width: ' + (expanded ? '28rem' : '10rem') + '; width: 100%; transition: max-width 0.18s ease-out;'">
			<div class="input-group input-group-sm">
				<span class="input-group-text bg-transparent border-end-0"><i class="bi bi-search"></i></span>
				<input
					type="text"
					class="form-control border-start-0"
					:placeholder="expanded ? 'Search rows…' : 'Search…'"
					title="Search the full materialized view — complements the browser's native Ctrl+F, which only scans visible rows. Press Enter to jump to the next match; Shift+Enter for the previous."
					v-model="query"
					@focus="onFocus"
					@blur="onBlur"
					@keydown="onKeydown"
					data-testid="navbar-search-input"
					aria-label="Search the current grid"
				/>
				<button v-if="query" type="button" class="btn btn-outline-secondary" @click="clearQuery" title="Clear" data-testid="navbar-search-clear">
					<i class="bi bi-x"></i>
				</button>
			</div>
			<!--
				Dropdown. Absolute-positioned beneath the input so it doesn't push navbar
				items around when it opens. Capped height + scroll so a large hit list stays
				usable. mousedown (not click) on hit buttons so blur doesn't dismiss before
				the handler fires — see onBlur's deferred close.
			-->
			<div
				v-if="dropdownOpen &amp;&amp; debouncedQuery"
				class="position-absolute mt-1 shadow-sm border bg-body rounded"
				style="top: 100%; left: 0; right: 0; max-height: 60vh; overflow-y: auto; z-index: 1050;"
				data-testid="navbar-search-dropdown"
			>
				<div v-if="!activeGrid" class="p-3 small text-muted">
					<i class="bi bi-info-circle me-1"></i>No active grid — open a cube query to search its rows.
				</div>
				<div v-else-if="hits.length === 0 &amp;&amp; coordinateHits.length === 0 &amp;&amp; schemaHits.length === 0" class="p-3 small text-muted">
					<i class="bi bi-search me-1"></i>No matches in the current view, in known column coordinates, or in column/measure names.
				</div>
				<!--
					Header pill: shows "match X of Y" once the user has jumped to a hit (or after
					their first Enter). Only rendered when row-hits exist — the coordinate-search
					section has its own header below. Mirrors the browser's native find bar so the
					cycling affordance is discoverable.
				-->
				<div v-else-if="hits.length > 0" class="d-flex align-items-center justify-content-between px-2 py-1 border-bottom small text-muted">
					<span v-if="currentHitIndex >= 0" data-testid="navbar-search-counter"> Match {{ currentHitIndex + 1 }} of {{ hits.length }} </span>
					<span v-else data-testid="navbar-search-counter">{{ hits.length }} match{{ hits.length === 1 ? "" : "es" }}</span>
					<span class="font-monospace" title="Press Enter to jump to the next match; Shift+Enter for the previous.">↵ next · ⇧↵ prev</span>
				</div>
				<ul class="list-group list-group-flush mb-0" v-if="hits.length > 0">
					<li
						v-for="(hit, index) in hits"
						:key="index"
						class="list-group-item list-group-item-action py-1 px-2 small d-flex align-items-center gap-2"
						:class="{ active: index === currentHitIndex }"
						@mousedown.prevent="onHitClick(hit, index)"
						data-testid="navbar-search-hit"
					>
						<span class="badge text-bg-light" :title="hit.kind">{{ hit.kind === "measure" ? "M" : "G" }}</span>
						<span class="font-monospace text-truncate flex-grow-1">
							<span class="text-muted">{{ hit.column }}</span>
							<span class="ms-2">{{ hit.formatted }}</span>
						</span>
						<span class="text-muted small">row {{ hit.row + 1 }}</span>
					</li>
				</ul>
				<!--
					Phase B1 — found-in-columns section. Lists column/coordinate hits from the
					SPA's session-grown coordinate cache. Click adds the column to the active
					grid's groupBy so the user can navigate to the data themselves. Shown
					whenever the cache has matches, regardless of whether the current view had
					row hits — the two answer different questions (where IS this value in the
					rendered rows, versus WHICH COLUMN has this value at all), so both are
					useful side-by-side.
				-->
				<div v-if="coordinateHits.length > 0" class="border-top px-2 py-1 small text-muted bg-body-tertiary">
					<i class="bi bi-funnel me-1"></i>Found in columns (click to add to groupBy)
				</div>
				<ul v-if="coordinateHits.length > 0" class="list-group list-group-flush mb-0" data-testid="navbar-search-coordinate-hits">
					<li
						v-for="(hit, index) in coordinateHits"
						:key="'coord-' + index"
						class="list-group-item list-group-item-action py-1 px-2 small d-flex align-items-center gap-2"
						@mousedown.prevent="onCoordinateHitClick(hit)"
						data-testid="navbar-search-coordinate-hit"
					>
						<span class="badge text-bg-secondary" title="coordinate">C</span>
						<span class="font-monospace text-truncate flex-grow-1">
							<span class="text-muted">{{ hit.column }}</span>
							<span class="ms-2">{{ hit.formatted }}</span>
						</span>
						<span class="text-muted small"><i class="bi bi-plus-circle"></i></span>
					</li>
				</ul>
				<!--
					Schema-name section: matching column / measure names that the user might
					want to add to the query. Cheap discovery affordance — clicking ticks the
					name in the queryModel (same effect as opening the wizard accordion and
					clicking the checkbox). Items already in the query are rendered muted with
					a "Already added" hint so the user doesn't waste a click.
				-->
				<div v-if="schemaHits.length > 0" class="border-top px-2 py-1 small text-muted bg-body-tertiary">
					<i class="bi bi-plus-square me-1"></i>Add to query
				</div>
				<ul v-if="schemaHits.length > 0" class="list-group list-group-flush mb-0" data-testid="navbar-search-schema-hits">
					<li
						v-for="(hit, index) in schemaHits"
						:key="'schema-' + index"
						class="list-group-item list-group-item-action py-1 px-2 small d-flex align-items-center gap-2"
						:class="{ disabled: hit.alreadyInQuery }"
						@mousedown.prevent="onSchemaHitClick(hit)"
						data-testid="navbar-search-schema-hit"
					>
						<span class="badge" :class="hit.kind === 'measure' ? 'text-bg-primary' : 'text-bg-success'" :title="hit.kind">
							{{ hit.kind === "measure" ? "Measure" : "Column" }}
						</span>
						<span class="font-monospace text-truncate flex-grow-1">{{ hit.name }}</span>
						<span v-if="hit.alreadyInQuery" class="text-muted small fst-italic">already added</span>
						<span v-else class="text-muted small"><i class="bi bi-plus-circle"></i></span>
					</li>
				</ul>
				<div v-if="hits.length >= 100" class="px-2 py-1 small text-muted border-top">
					Showing first 100 matches — narrow the query for more precision.
				</div>
				<!--
					Phase B2 — opt-in fan-out across every column of the cube. The Phase B1
					"Found in columns" section above only knows what the SPA has previously
					loaded; this button fetches the rest. Surfaced at the bottom of the
					dropdown (not the top) so it doesn't distract from instant client-side
					answers — the user only reaches for it when those answers fall short. The
					button hides when every column is already loaded (no work to do).
				-->
				<div v-if="activeGrid &amp;&amp; unloadedColumns.length > 0 &amp;&amp; fanOutState.phase === 'idle'" class="border-top px-2 py-1 small">
					<button
						type="button"
						class="btn btn-sm btn-link p-0 text-decoration-none"
						@mousedown.prevent="runFanOut"
						data-testid="navbar-search-fan-out"
					>
						<i class="bi bi-search-heart me-1"></i>Search across all {{ unloadedColumns.length }} column{{ unloadedColumns.length === 1 ? "" : "s"
						}}…
					</button>
					<div class="text-muted">May take a few seconds.</div>
				</div>
				<div v-else-if="fanOutState.phase === 'running'" class="border-top px-2 py-1 small text-muted" data-testid="navbar-search-fan-out-progress">
					<span class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
					Loading coordinates for {{ fanOutState.columnsLoaded }} column{{ fanOutState.columnsLoaded === 1 ? "" : "s" }}…
					<!--
						Indeterminate progress: the bulk endpoint is one HTTP call so we don't
						have a per-column completion signal — show a full-width animated bar to
						convey "work in flight" without lying about the percentage.
					-->
					<div class="progress mt-1" style="height: 3px;">
						<div class="progress-bar progress-bar-striped progress-bar-animated" role="progressbar" style="width: 100%;"></div>
					</div>
				</div>
				<div v-else-if="fanOutState.phase === 'done'" class="border-top px-2 py-1 small text-muted" data-testid="navbar-search-fan-out-done">
					<i class="bi bi-check2 me-1"></i>Searched {{ fanOutState.columnsLoaded }} additional column{{ fanOutState.columnsLoaded === 1 ? "" : "s" }}.
				</div>
			</div>
		</div>
	`,
};
