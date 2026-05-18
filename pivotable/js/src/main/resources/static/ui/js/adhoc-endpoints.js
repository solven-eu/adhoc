// @ts-check
import { ref, computed, watch } from "vue";
import { useRouter } from "vue-router";
import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";
import { useUserStore } from "./store-user.js";
import { usePreferencesStore } from "./store-preferences.js";

import LoginChip from "./login-chip.js";

import AdhocEndpoint from "./adhoc-endpoint.js";

import { decodeShareInput, findKnownEndpointId, buildQueryDestination, parseEndpointUrl } from "./adhoc-share-helper.js";

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
		const router = useRouter();

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

		// "Import shared view" flow — paste a URL or JSON produced by the Share tab; preview
		// the decoded payload, optionally register a new endpoint after explicit consent, and
		// navigate to the query view. The whole flow lives in one modal: paste → preview →
		// confirm → navigate.
		const importInput = ref("");
		const importError = ref("");
		/** @type {import("vue").Ref<{ endpoint: { url: string, name: string }, cubeId: string, query: Object } | null>} */
		const importPreview = ref(null);
		const importKnownEndpointId = ref("");

		const previewImport = function () {
			importError.value = "";
			importPreview.value = null;
			importKnownEndpointId.value = "";
			if (!importInput.value || !importInput.value.trim()) {
				importError.value = "Paste a share URL or JSON first.";
				return;
			}
			try {
				const decoded = decodeShareInput(importInput.value);
				importPreview.value = decoded;
				const knownId = findKnownEndpointId(decoded, store.endpoints, preferencesStore.localEndpoints);
				if (knownId) {
					importKnownEndpointId.value = knownId;
				}
			} catch (e) {
				importError.value = (e && e.message) || String(e);
			}
		};

		const closeImportModal = function () {
			const closeBtn = /** @type {HTMLElement | null} */ (document.querySelector('#importSharedViewModal [data-bs-dismiss="modal"]'));
			if (closeBtn) closeBtn.click();
		};

		const confirmImport = function () {
			const payload = importPreview.value;
			if (!payload) return;
			let endpointId = importKnownEndpointId.value;
			if (!endpointId) {
				const parts = parseEndpointUrl(payload.endpoint.url);
				if (!parts) {
					importError.value = "Endpoint URL " + payload.endpoint.url + " cannot be registered locally (sentinel form).";
					return;
				}
				try {
					const registered = preferencesStore.addLocalEndpoint({
						host: parts.host,
						port: parts.port,
						prefix: parts.prefix,
						name: payload.endpoint.name,
					});
					endpointId = registered.id;
				} catch (e) {
					importError.value = (e && e.message) || String(e);
					return;
				}
			}
			const dest = buildQueryDestination(endpointId, payload.cubeId, payload.query);
			closeImportModal();
			// Reset for next time so the modal opens fresh.
			importInput.value = "";
			importPreview.value = null;
			importKnownEndpointId.value = "";
			router.push({ path: dest.path, hash: dest.hash });
		};

		return {
			endpoints,
			newEndpoint,
			addError,
			addEndpoint,
			removeLocalEndpoint,
			importInput,
			importError,
			importPreview,
			importKnownEndpointId,
			previewImport,
			confirmImport,
		};
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
			<div class="mt-3 text-end d-flex gap-2 justify-content-end">
				<button
					type="button"
					class="btn btn-outline-secondary btn-sm"
					data-bs-toggle="modal"
					data-bs-target="#importSharedViewModal"
					data-testid="open-import-shared-view-modal"
				>
					<i class="bi bi-clipboard-plus me-1"></i>Import shared view
				</button>
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

			<!--
				Import-shared-view modal: paste a URL or JSON produced by the Share tab. The user
				clicks Preview, sees the decoded endpoint+cube+query, and confirms — at which
				point a missing endpoint is registered locally and the route navigates to the
				query view. Consent step mirrors the URL-receive flow (adhoc-share-receive.js).
			-->
			<div
				class="modal fade"
				id="importSharedViewModal"
				tabindex="-1"
				aria-labelledby="importSharedViewModalLabel"
				aria-hidden="true"
				data-testid="import-shared-view-card"
			>
				<div class="modal-dialog modal-dialog-centered modal-lg">
					<div class="modal-content">
						<div class="modal-header">
							<h5 class="modal-title" id="importSharedViewModalLabel"><i class="bi bi-clipboard-plus me-1"></i>Import shared view</h5>
							<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
						</div>
						<div class="modal-body">
							<label for="shareImportInput" class="form-label small text-muted mb-1">
								Paste a share URL (produced by the Share tab) or the JSON payload
							</label>
							<textarea
								id="shareImportInput"
								class="form-control font-monospace small"
								rows="6"
								v-model="importInput"
								data-testid="import-shared-view-input"
							></textarea>
							<div class="mt-2">
								<button type="button" class="btn btn-outline-primary btn-sm" @click="previewImport" data-testid="import-shared-view-preview">
									<i class="bi bi-eye me-1"></i>Preview
								</button>
							</div>

							<div v-if="importError" class="alert alert-danger small mt-3 mb-0" role="alert">{{importError}}</div>

							<div v-if="importPreview" class="mt-3" data-testid="import-shared-view-preview-block">
								<h6 class="small text-muted mb-2">Preview</h6>
								<dl class="row small mb-2">
									<dt class="col-sm-3">URL</dt>
									<dd class="col-sm-9 font-monospace">{{importPreview.endpoint.url}}</dd>
									<dt class="col-sm-3" v-if="importPreview.endpoint.name">Name</dt>
									<dd class="col-sm-9" v-if="importPreview.endpoint.name">{{importPreview.endpoint.name}}</dd>
									<dt class="col-sm-3">Cube</dt>
									<dd class="col-sm-9 font-monospace">{{importPreview.cubeId}}</dd>
								</dl>
								<div v-if="importKnownEndpointId" class="alert alert-info py-1 px-2 small mb-0">
									<i class="bi bi-check2-circle me-1"></i>Endpoint already registered — only the query will be loaded.
								</div>
								<div v-else class="alert alert-warning py-1 px-2 small mb-0">
									<i class="bi bi-exclamation-triangle me-1"></i>Endpoint not registered yet. Confirming below will register it locally.
								</div>
							</div>
						</div>
						<div class="modal-footer">
							<button type="button" class="btn btn-secondary btn-sm" data-bs-dismiss="modal">Cancel</button>
							<button
								type="button"
								class="btn btn-primary btn-sm"
								:disabled="!importPreview"
								@click="confirmImport"
								data-testid="import-shared-view-confirm"
							>
								<i class="bi bi-check2 me-1"></i>{{ importKnownEndpointId ? "Open" : "Register and open" }}
							</button>
						</div>
					</div>
				</div>
			</div>
		</div>
	`,
};
