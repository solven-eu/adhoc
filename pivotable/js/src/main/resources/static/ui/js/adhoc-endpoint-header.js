import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import AdhocEndpointChip from "./adhoc-endpoint-chip.js";

import AdhocLoading from "./adhoc-loading.js";

export default {
	components: {
		AdhocEndpointChip,
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

		// Force-refetch the endpoint schema (tables + cubes), bypassing the if-missing cache check the
		// page-load path uses. Useful when the underlying data source has been updated server-side and the
		// SPA is showing a stale schema.
		const reloadSchema = function () {
			store.loadEndpointSchemas(props.endpointId, null);
		};

		return { reloadSchema };
	},
	template: /* HTML */ `
		<div v-if="!endpoint || endpoint.error">
			<AdhocLoading :id="endpointId" type="endpoint" :loading="nbSchemaFetching > 0" :error="endpoint.error" />
		</div>
		<div v-else>
			<span>
				<span v-if="withDescription">
					<h1>
						<AdhocEndpointChip :endpointId="endpointId" />
						<button
							type="button"
							class="btn btn-sm btn-outline-secondary ms-2 align-baseline"
							:disabled="nbSchemaFetching > 0"
							@click="reloadSchema"
							title="Reload tables and cubes from this endpoint"
						>
							<span v-if="nbSchemaFetching > 0">
								<span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>
								Reloading…
							</span>
							<span v-else><i class="bi bi-arrow-clockwise"></i> Reload schema</span>
						</button>
					</h1>
					Endpoint-Description: {{endpoint.name}}
				</span>
				<span v-else>
					<h5>
						<AdhocEndpointChip :endpointId="endpointId" />
					</h5>
				</span>
			</span>
		</div>
	`,
};
