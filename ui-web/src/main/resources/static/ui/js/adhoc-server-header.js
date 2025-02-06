import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocServerRef from "./adhoc-server-ref.js";

export default {
	components: {
		AdhocServerRef,
	},
	props: {
		gameId: {
			type: String,
			required: true,
		},
		withDescription: {
			type: Boolean,
			default: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbGameFetching"]),
		...mapState(useAdhocStore, {
			game(store) {
				return store.games[this.gameId];
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadGameIfMissing(props.gameId);

		return {};
	},
	template: /* HTML */ `
        <div v-if="(!game) && (nbGameFetching > 0)">
            <div class="spinner-border" role="status">
                <span class="visually-hidden">Loading gameId={{gameId}}</span>
            </div>
        </div>
        <div v-else-if="game.error">game.error={{game.error}}</div>
        <div v-else>
            <span>
                <span v-if="withDescription">
                    <h1>
                        <AdhocServerRef :gameId="game.gameId" />
                        <!--RouterLink :to="{path:'/html/games'}"><i class="bi bi-arrow-90deg-left"></i></RouterLink-->
                    </h1>
                    Game-Description: {{game.shortDescription}}
                </span>
                <span v-else>
                    <h5>
                        <AdhocServerRef :gameId="gameId" />
                    </h5>
                </span>
            </span>
        </div>
    `,
};
