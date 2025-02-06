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
		gameId: {
			type: String,
			// required: true,
		},
		showGame: {
			type: Boolean,
			// As we show multiple contests, we do not show the game (by default)
			default: false,
		},
		showLeaderboard: {
			type: Boolean,
			// As we show multiple contests, we do not show the leaderboard (by default)
			default: false,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["isLoggedIn", "nbGameFetching", "nbCubeFetching"]),
		...mapState(useAdhocStore, {
			contests(store) {
				const allContests = Object.values(store.contests);

				console.debug("allContests", allContests);

				if (this.gameId) {
					// https://stackoverflow.com/questions/69091869/how-to-filter-an-array-in-array-of-objects-in-javascript
					return allContests.filter((contest) => contest.constantMetadata.gameId === this.gameId);
				} else {
					return allContests;
				}
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		if (props.gameId) {
			// The contests of a specific game
			store.loadContests(props.gameId);
		} else {
			// Cross-through contests
			store.loadContests();
		}

		return {};
	},

	template: /* HTML */ `
        <div v-if="!isLoggedIn"><LoginRef /></div>
        <div v-else-if="Object.values(contests).length == 0 && nbCubeFetching > 0">Loading cubes</div>
        <div v-else class="container">
            <div class="row border" v-for="contest in contests">
                <AdhocCube
                    :gameId="contest.constantMetadata.gameId"
                    :contestId="contest.contestId"
                    :showServer="showServer"
                    :showLeaderboard="showLeaderboard"
                />
            </div>
        </div>
    `,
};
