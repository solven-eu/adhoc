// @ts-check
import { ref, computed, watch } from "vue";
import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";
import { useUserStore } from "./store-user.js";
import { usePreferencesStore } from "./store-preferences.js";

import LoginChip from "./login-chip.js";

import AdhocEndpoint from "./adhoc-endpoint.js";

export default {
	components: {
		LoginChip,
		AdhocEndpoint,
	},
	computed: {
		// `needsToCheckLogin` lives on the user store (not the adhoc store); split accordingly.
		...mapState(useUserStore, ["needsToCheckLogin"]),
		...mapState(useAdhocStore, ["isLoggedIn", "nbSchemaFetching"]),
	},
	setup() {
		const store = useAdhocStore();
		const preferencesStore = usePreferencesStore();

		watch(
			() => store.isLoggedIn,
			(isLoggedIn) => {
				if (isLoggedIn) {
					store.loadEndpoints();
				} else {
				}
			},
			{
				// immediate to ensure endpoints loads when component is mounted, not just when logging-in
				immediate: true,
			},
		);

		// Endpoints displayed = server-discovered ∪ user-registered. The two sources are
		// merged at view time rather than copied into a single store slot, so a localStorage
		// edit (or a server round-trip) is reflected without resync logic.
		const endpoints = computed(() => {
			const merged = { ...preferencesStore.localEndpoints, ...store.endpoints };
			return Object.values(merged);
		});

		// "Register endpoint" form state. Defaults match the local dev backend so a user
		// can hit the button as soon as they reach this page (the friction-free test path).
		const newEndpoint = ref({ host: "127.0.0.1", port: 8080, prefix: "", name: "" });
		const addError = ref("");

		const addEndpoint = function () {
			addError.value = "";
			try {
				preferencesStore.addLocalEndpoint({ ...newEndpoint.value });
				newEndpoint.value = { host: "127.0.0.1", port: 8080, prefix: "", name: "" };
				// Dismiss the modal on success — Bootstrap exposes a programmatic close via the same
				// `data-bs-dismiss="modal"` mechanism we use on the X button. We trigger it by clicking
				// the close button in the header so we do not have to import the Modal JS class.
				const closeBtn = /** @type {HTMLElement | null} */ (document.querySelector('#registerEndpointModal [data-bs-dismiss="modal"]'));
				if (closeBtn) {
					closeBtn.click();
				}
			} catch (e) {
				addError.value = e.message || String(e);
			}
		};

		const removeLocalEndpoint = function (id) {
			preferencesStore.removeLocalEndpoint(id);
		};

		return { endpoints, newEndpoint, addError, addEndpoint, removeLocalEndpoint };
	},
	template: /* HTML */ `
		<div v-if="needsToCheckLogin">Loading the login status...</div>
		<div v-else-if="!isLoggedIn">
			Needs to be logged-in to fetch endpoints.
			<br />
			<LoginChip />
		</div>
		<div v-else class="container">
			<div v-if="endpoints.length === 0">
				<div v-if="nbSchemaFetching > 0">Loading endpoints…</div>
				<div v-else class="text-muted">No endpoints registered yet.</div>
			</div>
			<div v-else>
				<div class="row border" v-for="endpoint in endpoints" :key="endpoint.id" :data-testid="'endpoint-row-' + endpoint.id">
					<AdhocEndpoint :endpointId="endpoint.id" :showSchema="false" />
					<div v-if="endpoint.local" class="text-muted small mb-1">
						<i class="bi bi-pin-angle me-1"></i>Locally registered: <span class="font-monospace">{{endpoint.url}}</span>
						<button
							type="button"
							class="btn btn-link btn-sm text-danger ms-2 p-0"
							@click="removeLocalEndpoint(endpoint.id)"
							:data-testid="'remove-endpoint-' + endpoint.id"
						>
							<i class="bi bi-trash"></i> Remove
						</button>
					</div>
				</div>
			</div>

			<!--
				Register-an-endpoint affordance: a single trigger button that opens a modal carrying the
				form. The button lives BELOW the list (the user is here to browse the existing endpoints
				first; the "register a new one" action is secondary). The previous inline-form layout
				was demoted to a modal so it no longer competes with the endpoint list for attention.
			-->
			<div class="mt-3 text-end">
				<button
					type="button"
					class="btn btn-outline-primary btn-sm"
					data-bs-toggle="modal"
					data-bs-target="#registerEndpointModal"
					data-testid="open-register-endpoint-modal"
				>
					<i class="bi bi-plus-circle me-1"></i>Register an endpoint
				</button>
			</div>

			<!-- Register-endpoint modal -->
			<div
				class="modal fade"
				id="registerEndpointModal"
				tabindex="-1"
				aria-labelledby="registerEndpointModalLabel"
				aria-hidden="true"
				data-testid="register-endpoint-card"
			>
				<div class="modal-dialog modal-dialog-centered">
					<div class="modal-content">
						<div class="modal-header">
							<h5 class="modal-title" id="registerEndpointModalLabel"><i class="bi bi-plus-circle me-1"></i>Register an endpoint</h5>
							<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
						</div>
						<form @submit.prevent="addEndpoint">
							<div class="modal-body">
								<div class="row g-2">
									<div class="col-12 col-md-7">
										<label for="newEndpointHost" class="form-label small text-muted mb-1">Host</label>
										<input
											id="newEndpointHost"
											class="form-control form-control-sm"
											v-model="newEndpoint.host"
											placeholder="127.0.0.1"
											data-testid="new-endpoint-host"
										/>
									</div>
									<div class="col-12 col-md-5">
										<label for="newEndpointPort" class="form-label small text-muted mb-1">Port</label>
										<input
											id="newEndpointPort"
											class="form-control form-control-sm"
											type="number"
											min="1"
											max="65535"
											v-model.number="newEndpoint.port"
											data-testid="new-endpoint-port"
										/>
									</div>
									<div class="col-12 col-md-6">
										<label for="newEndpointPrefix" class="form-label small text-muted mb-1">Prefix (optional)</label>
										<input
											id="newEndpointPrefix"
											class="form-control form-control-sm"
											v-model="newEndpoint.prefix"
											placeholder="/api"
											data-testid="new-endpoint-prefix"
										/>
									</div>
									<div class="col-12 col-md-6">
										<label for="newEndpointName" class="form-label small text-muted mb-1">Display name (optional)</label>
										<input
											id="newEndpointName"
											class="form-control form-control-sm"
											v-model="newEndpoint.name"
											placeholder="auto"
											data-testid="new-endpoint-name"
										/>
									</div>
								</div>
								<div v-if="addError" class="alert alert-danger small mt-3 mb-0" role="alert">{{addError}}</div>
							</div>
							<div class="modal-footer">
								<button type="button" class="btn btn-secondary btn-sm" data-bs-dismiss="modal">Cancel</button>
								<button type="submit" class="btn btn-primary btn-sm" data-testid="new-endpoint-submit">Add</button>
							</div>
						</form>
					</div>
				</div>
			</div>
		</div>
	`,
};
