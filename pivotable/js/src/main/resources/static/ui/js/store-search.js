// @ts-check
import { defineStore } from "pinia";

/**
 * @typedef {Object} ActiveGridContext
 * @property {{ coordinates: Array<Record<string, any>>, values: Array<Record<string, any>> }} view
 * @property {string[]} coordinateColumns
 * @property {string[]} measureColumns
 * @property {(row: number, columnId: string) => string | null} formatCell
 * @property {(row: number, columnId?: string) => void} scrollToRow scrolls the SlickGrid to the given row (and optionally flashes the cell)
 * @property {string} cubeId for hit attribution in the modal
 * @property {string} endpointId
 */

// Lightweight session-scoped store that brokers the connection between the navbar search
// component (consumer) and the currently-rendered query grid (producer).
//
// `<AdhocQueryGrid>` registers itself in `onMounted` and unregisters in `onUnmounted`. The
// search modal reads `activeGrid` reactively; when null, the modal renders a "no active grid"
// placeholder so the user knows to navigate to a query first.
//
// The store is intentionally lean — no list of "recent searches", no cross-page cache. Phase B
// (the cross-column coordinate search) will add a separate slice; keeping Phase A minimal makes
// the seam obvious.
export const useSearchStore = defineStore("search", {
	state: () =>
		/** @type {{ activeGrid: ActiveGridContext | null }} */ ({
			activeGrid: null,
		}),
	actions: {
		/**
		 * Called by the grid component once SlickGrid is mounted and the data is rendered.
		 * Subsequent re-registrations replace the prior entry — typical case is a resync that
		 * rebuilds the column list, where the formatCell closure has to be refreshed.
		 *
		 * @param {ActiveGridContext} ctx
		 */
		registerActiveGrid(ctx) {
			this.activeGrid = ctx;
		},
		/**
		 * Called by the grid on unmount. We compare by cubeId+endpointId so a stale unmount
		 * from a previously-active grid doesn't clobber a freshly-mounted one (component
		 * order in Vue lifecycle isn't guaranteed across route swaps).
		 *
		 * @param {string} cubeId
		 * @param {string} endpointId
		 */
		clearActiveGrid(cubeId, endpointId) {
			const current = this.activeGrid;
			if (current && current.cubeId === cubeId && current.endpointId === endpointId) {
				this.activeGrid = null;
			}
		},
	},
});
