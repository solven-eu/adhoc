import {} from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

export default {
	components: {},
	props: {
		endpointId: {
			type: String,
			required: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, {
			endpoint(store) {
				return store.endpoints[this.endpointId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadEndpointIfMissing(props.endpointId);

		return {};
	},
	template: /* HTML */ ` <RouterLink :to="{path:'/html/endpoints/' + endpoint.id}"><i class="bi bi-cloud-check"></i> {{endpoint.name}}</RouterLink> `,
};
