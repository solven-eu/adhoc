import {} from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

export default {
	components: {},
	props: {
		gameId: {
			type: String,
			required: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, {
			game(store) {
				return store.games[this.gameId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadGameIfMissing(props.gameId);

		return {};
	},
	template: /* HTML */ ` <RouterLink :to="{path:'/html/games/' + game.gameId}"><i class="bi bi-puzzle"></i> {{game.title}}</RouterLink> `,
};
