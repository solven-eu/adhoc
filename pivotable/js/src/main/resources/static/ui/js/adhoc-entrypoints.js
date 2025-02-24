import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import LoginRef from "./login-ref.js";

import AdhocEntrypoint from "./adhoc-entrypoint.js";

export default {
	components: {
		LoginRef,
		AdhocEntrypoint,
	},
	computed: {
		...mapState(useAdhocStore, ["isLoggedIn", "nbEntrypointFetching"]),
		...mapState(useAdhocStore, {
			entrypoints(store) {
				return Object.values(store.entrypoints);
			},
		}),
	},
	setup() {
		const store = useAdhocStore();

		store.loadEntrypoints();

		return {};
	},
	template: /* HTML */ `
        <div v-if="!isLoggedIn"><LoginRef /></div>
        <div v-if="Object.keys(entrypoints).length == 0">
            <div v-if="nbEntrypointFetching > 0">Loading entrypoints</div>
            <div v-else>Issue loading entrypoints (or no entrypoints at all)</div>
        </div>
        <div v-else class="container">
            <div class="row border" v-for="entrypoint in entrypoints">
                <AdhocEntrypoint :entrypointId="entrypoint.id" :showSchema="false" v-if="!entrypoint.error" />
            </div>
        </div>
    `,
};
