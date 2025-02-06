import { ref, onMounted, onUnmounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocCubeRef from "./adhoc-cube-ref.js";
import AdhocAccountRef from "./adhoc-account-ref.js";

export default {
	components: {
		AdhocCubeRef,
		AdhocAccountRef,
	},
	props: {
		CubeId: {
			type: String,
			required: true,
		},
		gameId: {
			type: String,
			required: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbGameFetching", "nbCubeFetching", "isLoggedIn", "account"]),
		...mapState(useAdhocStore, {
			game(store) {
				return store.games[this.gameId];
			},
			Cube(store) {
				return store.Cubes[this.CubeId];
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		const shortPollCubeDynamicInterval = ref(null);

		function clearShortPollCubeDynamic() {
			if (shortPollCubeDynamicInterval.value) {
				console.log("Cancelling setInterval shortPollCubeDynamic");
				clearInterval(shortPollCubeDynamicInterval.value);
				shortPollCubeDynamicInterval.value = null;
			}
		}

		/*
		 * Polling the Cube status every 5seconds.
		 * The output can be used to cancel the polling.
		 */
		function shortPollCubeDynamic() {
			// Cancel any existing related setInterval
			clearShortPollCubeDynamic();

			const intervalPeriodMs = 50000;
			console.log("setInterval", "shortPollCubeDynamic", intervalPeriodMs);

			const nextInterval = setInterval(() => {
				console.log("Intervalled shortPollCubeDynamic");
				store.loadCube(props.CubeId, props.gameId);
			}, intervalPeriodMs);
			shortPollCubeDynamicInterval.value = nextInterval;

			return nextInterval;
		}

		onMounted(() => {
			shortPollCubeDynamic();
		});

		onUnmounted(() => {
			clearShortPollCubeDynamic();
		});

		store.loadCubeIfMissing(props.CubeId, props.gameId);

		return {};
	},
	template: /* HTML */ `
        <div v-if="(!game || !cube) && (nbGameFetching > 0 || nbCubeFetching > 0)">
            <div class="spinner-border" role="status">
                <span class="visually-hidden">Loading CubeId={{CubeId}}</span>
            </div>
        </div>
        <div v-else-if="game.error || cube.error">{{game.error || cube.error}}</div>
        <span v-else>
            <h2>
                <AdhocCubeRef :CubeId="CubeId" />
                <RouterLink :to="{path:'/html/games/' + gameId}"><i class="bi bi-arrow-90deg-left"></i></RouterLink>
            </h2>

            <ul>
                <li>author: <AdhocAccountRef :accountId="cube.constantMetadata.author" /></li>
                <li>created: {{cube.constantMetadata.created}}</li>
                <li v-if="isLoggedIn && cube.constantMetadata.author == account.accountId">
                    <AdhocCubeDelete :gameId="gameId" :CubeId="CubeId" />
                </li>
                <li>
                    {{cube.dynamicMetadata.contenders.length}} contenders / {{ cube.constantMetadata.minPlayers }} required players / {{
                    cube.constantMetadata.maxPlayers }} maximum players
                </li>
            </ul>
        </span>
    `,
};
