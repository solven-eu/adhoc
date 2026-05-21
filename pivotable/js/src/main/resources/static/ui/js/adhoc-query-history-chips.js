// @ts-check
//
// Phase 4 of the history roadmap: forward + backtrack suggestion chips, rendered as a small
// horizontal strip. Each chip click hydrates `queryModel` from the target snapshot — same
// mechanism the existing "Restore last successful query" banner uses, but driven by the
// persistent history graph instead of the in-session lastSuccessfulQuery ref.
//
// Surfacing strategy:
//   - Hidden when there's no signal (empty graph, or current node has no outgoing edges AND
//     no smaller-name-set ancestors). The chip strip earns its row only when it has something
//     useful to say.
//   - Forward chips on the left ("+ groupBy country"), backtrack chips on the right
//     ("− filter"). Visual asymmetry mirrors the temporal direction ("→ next" vs "← simpler").
//   - Recomputed on every `bumpVersion` tick so chips stay current as the user builds up
//     history within a session.

import { computed } from "vue";

import { backtrackSuggestions, contentHash, forwardSuggestions, useQueryHistoryStore } from "./adhoc-query-history-store.js";

export default {
	props: {
		cubeId: {
			type: String,
			required: true,
		},
		// Current queryModel snapshot (parsedJson). The chips are anchored to the node that
		// matches this snapshot's content hash; suggestions are read off the outgoing edges
		// of that node (forward) and off subset-nodes (backtrack).
		currentSnapshot: {
			type: Object,
			default: null,
		},
		// Bump counter from the parent — incremented on every successful capture so the
		// computeds below re-derive without us reaching into reactivity internals.
		bumpVersion: {
			type: Number,
			default: 0,
		},
	},
	emits: ["restore"],
	setup(props, { emit }) {
		const history = useQueryHistoryStore(props.cubeId);

		// Re-read the snapshot on each bump. `void props.bumpVersion` creates the reactive dep
		// without us using its value — that's the only signal we need to invalidate.
		const snapshot = computed(() => {
			void props.bumpVersion;
			return history.snapshot();
		});

		const currentHash = computed(() => (props.currentSnapshot ? contentHash(props.currentSnapshot) : null));

		const forwardChips = computed(() => {
			if (!currentHash.value) return [];
			return forwardSuggestions(snapshot.value, currentHash.value, { topN: 3 });
		});

		const backtrackChips = computed(() => {
			if (!currentHash.value) return [];
			return backtrackSuggestions(snapshot.value, currentHash.value, { topN: 2 });
		});

		const onRestore = (snap) => {
			emit("restore", snap);
		};

		return {
			forwardChips,
			backtrackChips,
			onRestore,
		};
	},
	template: /* HTML */ `
		<div v-if="forwardChips.length > 0 || backtrackChips.length > 0" class="d-flex flex-wrap align-items-center gap-2 small">
			<!--
				Backtrack chips (left). "← simpler" mental model: peel layers off the current query.
				Rendered first so they sit on the LEFT, matching reading order for the "go back" motion.
			-->
			<template v-if="backtrackChips.length > 0">
				<span class="text-muted">← simpler:</span>
				<button
					v-for="chip in backtrackChips"
					:key="chip.toHash"
					type="button"
					class="btn btn-sm btn-outline-secondary py-0 px-2"
					style="font-size: 0.78rem;"
					@click="onRestore(chip.node.queryModelJson)"
					:title="'Restore a previous query you ran with these axes — ' + chip.label"
				>
					<i class="bi bi-arrow-90deg-left me-1"></i>{{ chip.label }}
				</button>
			</template>

			<!-- Vertical separator when both groups are present, so the two motions read distinct. -->
			<span v-if="forwardChips.length > 0 &amp;&amp; backtrackChips.length > 0" class="text-muted">·</span>

			<!--
				Forward chips (right). "→ often next" mental model: actions the user typically took
				from the current node. Score on each chip = recency × frequency of the outgoing edge.
			-->
			<template v-if="forwardChips.length > 0">
				<span class="text-muted">often next:</span>
				<button
					v-for="chip in forwardChips"
					:key="chip.toHash"
					type="button"
					class="btn btn-sm btn-outline-primary py-0 px-2"
					style="font-size: 0.78rem;"
					@click="onRestore(chip.target.queryModelJson)"
					:title="'Restore the query you often build next from here — ' + chip.label"
				>
					{{ chip.label }}<i class="bi bi-arrow-right-short ms-1"></i>
				</button>
			</template>
		</div>
	`,
};
