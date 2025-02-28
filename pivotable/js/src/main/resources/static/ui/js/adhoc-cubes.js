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
		endpointId: {
			type: String,
			// required: true,
		},
		showEndpoint: {
			type: Boolean,
			// As we show multiple contests, we do not show the endpoint (by default)
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

				if (this.endpointId) {
					// https://stackoverflow.com/questions/69091869/how-to-filter-an-array-in-array-of-objects-in-javascript
					return allContests.filter((contest) => contest.constantMetadata.endpointId === this.endpointId);
				} else {
					return allContests;
				}
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		if (props.endpointId) {
			// The contests of a specific endpoint
			store.loadSchemas(props.endpointId);
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
                    :endpointId="contest.constantMetadata.endpointId"
                    :contestId="contest.contestId"
                    :showEndpoint="showEndpoint"
                    :showLeaderboard="showLeaderboard"
                />
            </div>
        </div>
    `,
};
