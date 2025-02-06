import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocServerHeader from "./adhoc-server-header.js";
import AdhocCubeHeader from "./adhoc-cube-header.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocServerHeader,
		AdhocCubeHeader,
	},
	// https://vuejs.org/guide/components/props.html
	props: {
		contestId: {
			type: String,
			required: true,
		},
		gameId: {
			type: String,
			required: true,
		},
		showGame: {
			type: Boolean,
			default: true,
		},
		showLeaderboard: {
			type: Boolean,
			default: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbGameFetching", "nbContestFetching"]),
		...mapState(useAdhocStore, {
			game(store) {
				return store.games[this.gameId];
			},
			contest(store) {
				return store.contests[this.contestId];
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadContestIfMissing(props.contestId, props.gameId);

		return {};
	},
	template: /* HTML */ `
        <div v-if="(!game || !contest)">
            <div v-if="(nbGameFetching > 0 || nbContestFetching > 0)">
                <div class="spinner-border" role="status">
                    <span class="visually-hidden">Loading contestId={{contestId}}</span>
                </div>
            </div>
            <div v-else>
                <span>Issue loading contestId={{contestId}}</span>
            </div>
        </div>
        <div v-else-if="game.error || contest.error">{{game.error || contest.error}}</div>
        <div v-else>
            <AdhocCubeHeader :gameId="gameId" :contestId="contestId" />

            <AdhocServerHeader :gameId="gameId" :withDescription="false" v-if="showGame" />

            <RouterLink :to="{path:'/html/games/' + gameId + '/contest/' + contestId + '/board'}">
                <button type="button" class="btn btn-outline-primary">Preview the board</button>
            </RouterLink>
        </div>
    `,
};
