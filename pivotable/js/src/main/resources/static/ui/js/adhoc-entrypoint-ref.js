import {} from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

export default {
	components: {},
	props: {
		entrypointId: {
			type: String,
			required: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, {
			entrypoint(store) {
				return store.entrypoints[this.entrypointId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadEntrypointIfMissing(props.entrypointId);

		return {};
	},
	template: /* HTML */ ` <RouterLink :to="{path:'/html/entrypoints/' + entrypoint.id}"><i class="bi bi-puzzle"></i> {{entrypoint.name}}</RouterLink> `,
};
