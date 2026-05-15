// @ts-check
import { ref, onBeforeUnmount } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

/**
 * Top-level banner that surfaces the public-metadata probe state. Hidden while the probe is
 * `idle`/`ok`, shows a transient hint on the very first `loading`, and pins a red dismiss-less
 * alert (with manual + auto-retry countdown) once the probe enters `error`.
 *
 * The retry button calls `store.loadMetadata()` directly — the store cancels the pending
 * scheduled retry on entry, so manual + auto cannot stack.
 */
export default {
	computed: {
		...mapState(useAdhocStore, ["backendStatus"]),
		secondsUntilRetry() {
			// pinia's mapState typing surfaces `this.backendStatus` as the raw state factory, not
			// the unwrapped object — same dance as the other components in this tree (see
			// `pinia` note in `types/ambient.d.ts`). Cast through `any` to read the nested field.
			const self = /** @type {any} */ (this);
			if (!self.backendStatus.retryAt) {
				return null;
			}
			const ms = self.backendStatus.retryAt - self.now;
			return ms > 0 ? Math.ceil(ms / 1000) : 0;
		},
	},
	setup() {
		// `now` ticks every second so the countdown re-renders without the store having to repaint
		// `retryAt` itself. A single ref shared by every render path is cheaper than a watcher per use.
		const now = ref(Date.now());
		const tick = setInterval(() => {
			now.value = Date.now();
		}, 1000);
		onBeforeUnmount(() => {
			clearInterval(tick);
		});

		const store = useAdhocStore();
		const retryNow = () => {
			store.loadMetadata();
		};

		return { now, retryNow };
	},
	template: /* HTML */ `
		<div
			v-if="backendStatus.phase === 'error' || (backendStatus.phase === 'loading' && backendStatus.attempt > 1)"
			class="alert alert-danger d-flex align-items-center justify-content-between mb-2"
			role="alert"
		>
			<div class="d-flex align-items-center gap-2">
				<i class="bi bi-plug" aria-hidden="true"></i>
				<div v-if="backendStatus.phase === 'loading'">
					<strong>Reconnecting to backend…</strong>
					<span class="ms-1 text-muted small">attempt {{backendStatus.attempt}}</span>
				</div>
				<div v-else>
					<strong>Backend unreachable.</strong>
					<span class="ms-1" v-if="backendStatus.http">{{backendStatus.http}} {{backendStatus.message}}</span>
					<span class="ms-1" v-else>{{backendStatus.message}}</span>
					<span class="ms-1 text-muted small" v-if="secondsUntilRetry !== null">
						Retrying in {{secondsUntilRetry}}s (attempt {{backendStatus.attempt}})
					</span>
				</div>
			</div>
			<button type="button" class="btn btn-sm btn-outline-light" @click="retryNow" :disabled="backendStatus.phase === 'loading'">
				<span class="spinner-border spinner-border-sm me-1" v-if="backendStatus.phase === 'loading'"></span>
				Retry now
			</button>
		</div>
	`,
};
