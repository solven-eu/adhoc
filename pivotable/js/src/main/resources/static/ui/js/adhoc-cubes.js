import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import LoginRef from "./login-ref.js";
import AdhocCube from "./adhoc-cube.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		LoginRef,
		AdhocCube,
	},
	// https://vuejs.org/guide/components/props.html
	props: {
		entrypointId: {
			type: String,
			// required: true,
		},
		showEntrypoint: {
			type: Boolean,
			// As we show multiple contests, we do not show the entrypoint (by default)
			default: false,
		},
		showLeaderboard: {
			type: Boolean,
			// As we show multiple contests, we do not show the leaderboard (by default)
			default: false,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["isLoggedIn", "nbSchemaFetching", "nbCubeFetching"]),
		...mapState(useAdhocStore, {
			contests(store) {
				const allContests = Object.values(store.contests);

				console.debug("allContests", allContests);

				if (this.entrypointId) {
					// https://stackoverflow.com/questions/69091869/how-to-filter-an-array-in-array-of-objects-in-javascript
					return allContests.filter((contest) => contest.constantMetadata.entrypointId === this.entrypointId);
				} else {
					return allContests;
				}
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		if (props.entrypointId) {
			// The contests of a specific entrypoint
			store.loadSchemas(props.entrypointId);
		} else {
			// Cross-through contests
			store.loadSchemas();
		}

		return {};
	},

	template: /* HTML */ `
        <div v-if="!isLoggedIn"><LoginRef /></div>
        <div v-else-if="Object.values(contests).length == 0 && nbCubeFetching > 0">Loading cubes</div>
        <div v-else class="container">
            <div class="row border" v-for="contest in contests">
                <AdhocCube
                    :entrypointId="contest.constantMetadata.entrypointId"
                    :contestId="contest.contestId"
                    :showEntrypoint="showEntrypoint"
                    :showLeaderboard="showLeaderboard"
                />
            </div>
        </div>
    `,
};
