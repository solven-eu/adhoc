// @ts-check
import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import AdhocEndpointChip from "./adhoc-endpoint-chip.js";
import AdhocEndpointActuator from "./adhoc-endpoint-actuator.js";

import AdhocLoading from "./adhoc-loading.js";

export default {
	components: {
		AdhocEndpointChip,
		AdhocEndpointActuator,
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
				const self = /** @type {any} */ (this);
				return store.endpoints[self.endpointId];
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
			<!--
				The big H1 + endpoint-chip + "Endpoint-Description" line that used to live here
				has moved into the navbar breadcrumb (Endpoints / endpoint name). What remains
				in the body is a compact toolbar — only the actuator status strip — so
				functionality stays reachable but the grid below gets the vertical space back.
				The Reload-schema action that used to live here now sits next to the "Show
				schema" chip at the bottom of AdhocEndpointSchema's summary view. The
				withDescription=false caller (the small in-listing endpoint chip in
				adhoc-endpoints.js) keeps its tiny H5 form for backward compatibility, although
				the navbar breadcrumb has made that variant largely redundant.
			-->
			<span v-if="withDescription">
				<div class="d-flex flex-wrap align-items-center gap-2">
					<AdhocEndpointActuator v-if="endpoint.url" :endpointUrl="endpoint.url" />
				</div>
			</span>
			<span v-else>
				<h5>
					<AdhocEndpointChip :endpointId="endpointId" />
				</h5>
			</span>
		</div>
	`,
};
