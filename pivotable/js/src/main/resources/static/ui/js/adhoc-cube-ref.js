import {} from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

export default {
	components: {},
	props: {
		contestId: {
			type: String,
			required: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, {
			contest(store) {
				return store.contests[this.contestId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadContestIfMissing(props.contestId);

		return {};
	},
	template: /* HTML */ `
        <RouterLink :to="{path:'/html/servers/' + contest.constantMetadata.entrypointId + '/cubes/' + contestId}">
            <i class="bi bi-trophy"></i> {{contest.constantMetadata.name}}
        </RouterLink>
    `,
};
