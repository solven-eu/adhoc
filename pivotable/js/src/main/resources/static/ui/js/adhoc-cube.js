import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocEntrypointHeader from "./adhoc-entrypoint-header.js";
import AdhocCubeHeader from "./adhoc-cube-header.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocEntrypointHeader,
		AdhocCubeHeader,
	},
	// https://vuejs.org/guide/components/props.html
	props: {
		contestId: {
			type: String,
			required: true,
		},
		entrypointId: {
			type: String,
			required: true,
		},
		showEntrypoint: {
			type: Boolean,
			default: true,
		},
		showLeaderboard: {
			type: Boolean,
			default: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbEntrypointFetching", "nbContestFetching"]),
		...mapState(useAdhocStore, {
			entrypoint(store) {
				return store.entrypoints[this.entrypointId];
			},
			contest(store) {
				return store.contests[this.contestId];
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadContestIfMissing(props.contestId, props.entrypointId);

		return {};
	},
	template: /* HTML */ `
        <div v-if="(!entrypoint || !contest)">
            <div v-if="(nbEntrypointFetching > 0 || nbContestFetching > 0)">
                <div class="spinner-border" role="status">
                    <span class="visually-hidden">Loading contestId={{contestId}}</span>
                </div>
            </div>
            <div v-else>
                <span>Issue loading contestId={{contestId}}</span>
            </div>
        </div>
        <div v-else-if="entrypoint.error || contest.error">{{entrypoint.error || contest.error}}</div>
        <div v-else>
            <AdhocCubeHeader :entrypointId="entrypointId" :contestId="contestId" />

            <AdhocEntrypointHeader :entrypointId="entrypointId" :withDescription="false" v-if="showEntrypoint" />

            <RouterLink :to="{path:'/html/entrypoints/' + entrypointId + '/contest/' + contestId + '/board'}">
                <button type="button" class="btn btn-outline-primary">Preview the board</button>
            </RouterLink>
        </div>
    `,
};
