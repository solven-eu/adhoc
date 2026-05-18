// @ts-check
import { ref } from "vue";
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

		// Reload-schema progress: 0..1 while the download is streaming, set back to 0 after the
		// reactor resolves. Drives the `<progress>` bar rendered next to the Reload button so
		// the user sees actual byte-by-byte progress instead of just a spinner — large schemas
		// (tens of cubes) take seconds to download and a static spinner felt unresponsive.
		const reloadPercent = ref(0);
		const reloadInFlight = ref(false);

		// Force-refetch the endpoint schema (tables + cubes), bypassing the if-missing cache check the
		// page-load path uses. Useful when the underlying data source has been updated server-side and the
		// SPA is showing a stale schema.
		const reloadSchema = function () {
			reloadPercent.value = 0;
			reloadInFlight.value = true;
			store
				.loadEndpointSchemas(props.endpointId, null, (currentBytes, done, percent) => {
					if (typeof percent === "number" && Number.isFinite(percent)) {
						reloadPercent.value = percent;
					}
				})
				.finally(() => {
					reloadInFlight.value = false;
					// Hold the bar at 100% for one tick so the eye registers completion, then reset.
					setTimeout(() => {
						if (!reloadInFlight.value) reloadPercent.value = 0;
					}, 300);
				});
		};

		return { reloadSchema, reloadPercent, reloadInFlight };
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
					<!--
						Live download progress for the schema reload. Hidden when not in-flight so the
						strip doesn't permanently occupy vertical space. Width animates from 0 → 100 %
						as the streaming reader hits each chunk (driven by store.toJSON's onProgress
						callback wired in setup()).
					-->
					<div v-if="reloadInFlight || reloadPercent > 0" class="progress my-1" style="height: 4px;" data-testid="endpoint-reload-progress">
						<div
							class="progress-bar progress-bar-striped progress-bar-animated bg-secondary"
							:style="'width: ' + Math.round(reloadPercent * 100) + '%;'"
							role="progressbar"
							:aria-valuenow="Math.round(reloadPercent * 100)"
							aria-valuemin="0"
							aria-valuemax="100"
						></div>
					</div>
					Endpoint-Description: {{endpoint.name}}
					<AdhocEndpointActuator v-if="endpoint.url" :endpointUrl="endpoint.url" />
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
