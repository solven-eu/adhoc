import { ref } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

export default {
	props: {
		entrypointId: {
			type: String,
			required: true,
		},
	},
	setup(props) {
		const store = useAdhocStore();

		return {};
	},
	template: /* HTML */ `
        <RouterLink :to="{path:'/html/entrypoints/' + entrypointId + '/schema'}"><i class="bi bi-node-plus"></i> Show schema</RouterLink>
    `,
};
