// @ts-check
import { computed } from "vue";
import { useRoute } from "vue-router";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

// Compact navbar breadcrumb that replaces the body-side `<AdhocEndpointHeader>` /
// `<AdhocCubeHeader>` titles. Reads the current route's `endpointId` / `cubeId` params and
// renders one of three shapes:
//
//   `Endpoints`                                 - on /html/endpoints (no current endpoint)
//   `Endpoints › <endpoint name>`               - on /html/endpoints/:id and friends
//   `Endpoints › <endpoint name> › <cube name>` - on /html/endpoints/:id/cubes/:cubeId[/query]
//
// Segments are clickable links to the corresponding view; the trailing segment is rendered
// as plain text (the current location) to mirror the standard Bootstrap breadcrumb idiom.
// Falls back to the raw id when the store hasn't yet loaded the endpoint/cube descriptor —
// the first paint then upgrades to the human name once the load resolves.
export default {
	computed: {
		...mapState(useAdhocStore, ["endpoints", "schemas"]),
	},
	setup() {
		const route = useRoute();

		const endpointId = computed(() => {
			const p = route && route.params && route.params.endpointId;
			return p ? String(p) : "";
		});
		const cubeId = computed(() => {
			const p = route && route.params && route.params.cubeId;
			return p ? String(p) : "";
		});

		return { endpointId, cubeId };
	},
	template: /* HTML */ `
		<nav aria-label="breadcrumb" class="d-flex align-items-center small">
			<ol class="breadcrumb m-0 align-items-center">
				<li class="breadcrumb-item" :class="{ active: !endpointId }" :aria-current="endpointId ? null : 'page'">
					<RouterLink v-if="endpointId" to="/html/endpoints" class="text-decoration-none"><i class="bi bi-puzzle me-1"></i>Endpoints</RouterLink>
					<span v-else><i class="bi bi-puzzle me-1"></i>Endpoints</span>
				</li>
				<li v-if="endpointId" class="breadcrumb-item" :class="{ active: !cubeId }" :aria-current="cubeId ? null : 'page'">
					<RouterLink v-if="cubeId" :to="{ path: '/html/endpoints/' + endpointId }" class="text-decoration-none">
						<i class="bi bi-cloud-check me-1"></i>{{ endpoints[endpointId]?.name || endpointId }}
					</RouterLink>
					<span v-else><i class="bi bi-cloud-check me-1"></i>{{ endpoints[endpointId]?.name || endpointId }}</span>
				</li>
				<li v-if="cubeId" class="breadcrumb-item active" aria-current="page"><i class="bi bi-box me-1"></i>{{ cubeId }}</li>
			</ol>
		</nav>
	`,
};
