import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocEntrypointRef from "./adhoc-entrypoint-ref.js";

export default {
	components: {
		AdhocEntrypointRef,
	},
	props: {
		entrypointId: {
			type: String,
			required: true,
		},
		withDescription: {
			type: Boolean,
			default: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbEntrypointFetching"]),
		...mapState(useAdhocStore, {
			entrypoint(store) {
				return store.entrypoints[this.entrypointId];
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadEntrypointIfMissing(props.entrypointId);

		return {};
	},
	template: /* HTML */ `
        <div v-if="(!entrypoint) && (nbEntrypointFetching > 0)">
            <div class="spinner-border" role="status">
                <span class="visually-hidden">Loading entrypointId={{entrypointId}}</span>
            </div>
        </div>
        <div v-else-if="entrypoint.error">entrypoint.error={{entrypoint.error}}</div>
        <div v-else>
            <span>
                <span v-if="withDescription">
                    <h1>
                        <AdhocEntrypointRef :entrypointId="entrypoint.id" />
                    </h1>
                    Entrypoint-Description: {{entrypoint.name}}
                </span>
                <span v-else>
                    <h5>
                        <AdhocEntrypointRef :entrypointId="entrypointId" />
                    </h5>
                </span>
            </span>
        </div>
    `,
};
