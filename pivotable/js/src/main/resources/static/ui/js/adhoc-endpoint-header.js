import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import AdhocEndpointRef from "./adhoc-endpoint-ref.js";

import AdhocLoading from "./adhoc-loading.js";

export default {
	components: {
		AdhocEndpointRef,
		AdhocLoading,
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
		<div v-if="!endpoint || endpoint.error">
		    <AdhocLoading :id="endpointId" type="endpoint" :loading="nbSchemaFetching > 0" :error="endpoint.error" />
		</div>
        <div v-else>
            <span>
                <span v-if="withDescription">
                    <h1>
                        <AdhocEndpointRef :endpointId="endpointId" />
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
