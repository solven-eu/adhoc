import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocEndpointRef from "./adhoc-endpoint-ref.js";

export default {
	components: {
		AdhocEndpointRef,
	},
	props: {
		endpointId: {
			type: String,
			required: true,
		},
		withDescription: {
			type: Boolean,
			default: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbSchemaFetching"]),
		...mapState(useAdhocStore, {
			endpoint(store) {
				return store.endpoints[this.endpointId];
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadEndpointIfMissing(props.endpointId);

		return {};
	},
	template: /* HTML */ `
        <div v-if="(!endpoint) && (nbSchemaFetching > 0)">
            <div class="spinner-border" role="status">
                <span class="visually-hidden">Loading endpointId={{endpointId}}</span>
            </div>
        </div>
        <div v-else-if="endpoint.error">endpoint.error={{endpoint.error}}</div>
        <div v-else>
            <span>
                <span v-if="withDescription">
                    <h1>
                        <AdhocEndpointRef :endpointId="endpoint.id" />
                    </h1>
                    Endpoint-Description: {{endpoint.name}}
                </span>
                <span v-else>
                    <h5>
                        <AdhocEndpointRef :endpointId="endpointId" />
                    </h5>
                </span>
            </span>
        </div>
    `,
};
