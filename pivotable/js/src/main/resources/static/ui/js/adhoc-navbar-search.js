// @ts-check
import { ref, computed, watch } from "vue";

import { useSearchStore } from "./store-search.js";
import { searchTabularView } from "./adhoc-search-helper.js";

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
			activeGrid,
			dropdownOpen,
			expanded,
			currentHitIndex,
			onFocus,
			onBlur,
			onHitClick,
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
				<div v-else-if="hits.length === 0" class="p-3 small text-muted"><i class="bi bi-search me-1"></i>No matches in the current view.</div>
				<!--
					Header pill: shows "match X of Y" once the user has jumped to a hit (or after
					their first Enter). Mirrors the browser's native find bar so the cycling
					affordance is discoverable.
				-->
				<div v-else class="d-flex align-items-center justify-content-between px-2 py-1 border-bottom small text-muted">
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
				<div v-if="hits.length >= 100" class="px-2 py-1 small text-muted border-top">
					Showing first 100 matches — narrow the query for more precision.
				</div>
			</div>
		</div>
	`,
};
