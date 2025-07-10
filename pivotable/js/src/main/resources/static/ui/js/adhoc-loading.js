import { mapState } from "pinia";

import { useAdhocStore } from "./store-adhoc.js";
import { useUserStore } from "./store-user.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {},
	// https://vuejs.org/guide/components/props.html
	props: {
		id: {
			type: String,
			required: true,
		},
		loading: {
			type: Boolean,
			required: true,
		},
		error: {
			type: String,
			required: false,
		},
	},
	computed: {
		...mapState(useUserStore, ["nbLoginLoading"]),
		...mapState(useAdhocStore, ["nbSchemaFetching"]),
	},
	setup() {
		return {};
	},
	template: /* HTML */ `
        <div v-if="loading">
			<div class="spinner-grow" role="status">
			    <span class="visually-hidden">Loading endpoint/cube information</span>
			</div>
			Loading id={{id}}
		</div>
        <div v-else-if="error && !error.endsWith('not_loaded')">
			<span>Issue loading id={{id}}. error={{error}}</span>
        </div>
    `,
};
