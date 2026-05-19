// @ts-check
import { ref, onMounted } from "vue";
import { useRouter } from "vue-router";

import { useAdhocStore } from "./store-adhoc.js";
import { usePreferencesStore } from "./store-preferences.js";
import { decodeShareInput, findKnownEndpointId, buildQueryDestination, parseEndpointUrl } from "./adhoc-share-helper.js";

// Landing component for the /html/share?... URL form. Reads the share payload from the URL
// hash, surfaces a one-shot consent screen when the endpoint is not yet registered, and routes
// to the cube's query view once the user confirms.
//
// The consent step exists because silently registering a remote endpoint on link-open is a soft
// phishing vector: a shared URL could point at an attacker-controlled host. The UI surfaces
// (host, port, prefix) verbatim so the user can decide.
export default {
	setup() {
		const router = useRouter();
		const adhocStore = useAdhocStore();
		const preferencesStore = usePreferencesStore();

		/** @type {import("vue").Ref<string>} */
		const errorMessage = ref("");
		/** @type {import("vue").Ref<Object | null>} */
		const payload = ref(null);
		/** @type {import("vue").Ref<"loading" | "known" | "consent" | "registering" | "navigating" | "error">} */
		const phase = ref("loading");

		const readHash = function () {
			if (typeof window === "undefined" || !window.location || !window.location.hash) {
				return "";
			}
			return window.location.hash;
		};

		const navigateToQuery = function (endpointId) {
			const decoded = payload.value;
			if (!decoded) return;
			phase.value = "navigating";
			// `cubeId` and `endpointId` go in the path; `query` rides in the hash so the
			// destination view's existing hash-to-queryModel hydrator picks it up.
			const dest = buildQueryDestination(endpointId, decoded.cubeId, decoded.query);
			router.replace({ path: dest.path, hash: dest.hash });
		};

		const confirmRegister = function () {
			const decoded = payload.value;
			if (!decoded) return;
			const parts = parseEndpointUrl(decoded.endpoint.url);
			if (!parts) {
				// The :self sentinel ends up here. We never reach this branch in practice because
				// the recipient's own self-entry matches by url on mount, but surface a useful
				// message if it ever does.
				errorMessage.value = "Endpoint URL " + decoded.endpoint.url + " cannot be registered locally (sentinel form).";
				phase.value = "error";
				return;
			}
			phase.value = "registering";
			try {
				const endpoint = preferencesStore.addLocalEndpoint({
					host: parts.host,
					port: parts.port,
					prefix: parts.prefix,
					name: decoded.endpoint.name,
				});
				navigateToQuery(endpoint.id);
			} catch (e) {
				errorMessage.value = e && e.message ? String(e.message) : String(e);
				phase.value = "error";
			}
		};

		const cancel = function () {
			router.replace("/html/endpoints");
		};

		onMounted(async () => {
			let decoded;
			try {
				decoded = decodeShareInput(readHash());
			} catch (e) {
				errorMessage.value = e && e.message ? String(e.message) : String(e);
				phase.value = "error";
				return;
			}
			payload.value = decoded;
			// Ensure server-discovered endpoints are loaded before the match check. Otherwise the
			// :self sentinel (and any other already-discovered endpoint) would falsely fall through
			// to the consent screen on a cold-load /html/share visit.
			if (adhocStore.isLoggedIn && typeof adhocStore.loadEndpoints === "function") {
				try {
					await adhocStore.loadEndpoints();
				} catch (e) {
					// Best-effort — fall through to the match check with whatever's in the store.
					console.warn("loadEndpoints failed during share-receive", e);
				}
			}
			const knownId = findKnownEndpointId(decoded, adhocStore.endpoints, preferencesStore.localEndpoints);
			if (knownId) {
				phase.value = "known";
				navigateToQuery(knownId);
			} else {
				phase.value = "consent";
			}
		});

		return { phase, payload, errorMessage, confirmRegister, cancel };
	},
	template: /* HTML */ `
		<div class="container py-4">
			<div v-if="phase === 'loading'" class="text-muted">
				<span class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
				Loading shared view…
			</div>

			<div v-else-if="phase === 'error'" class="alert alert-danger" role="alert" data-testid="share-error">
				<h5><i class="bi bi-exclamation-triangle me-1"></i>Could not load shared view</h5>
				<div class="small">{{errorMessage}}</div>
				<button type="button" class="btn btn-outline-secondary btn-sm mt-2" @click="cancel">Back to endpoints</button>
			</div>

			<div v-else-if="phase === 'consent' && payload" data-testid="share-consent">
				<h4><i class="bi bi-link-45deg me-1"></i>Register shared endpoint?</h4>
				<p class="text-muted small mb-3">
					Someone shared a Pivotable view with you. The endpoint below is not registered on this browser. Confirm before proceeding — a malicious link
					could point at an untrusted host.
				</p>
				<dl class="row small">
					<dt class="col-sm-3">URL</dt>
					<dd class="col-sm-9 font-monospace">{{payload.endpoint.url}}</dd>
					<dt class="col-sm-3" v-if="payload.endpoint.name">Name</dt>
					<dd class="col-sm-9" v-if="payload.endpoint.name">{{payload.endpoint.name}}</dd>
					<dt class="col-sm-3">Cube</dt>
					<dd class="col-sm-9 font-monospace">{{payload.cubeId}}</dd>
				</dl>
				<div class="d-flex gap-2">
					<button type="button" class="btn btn-primary" @click="confirmRegister" data-testid="share-consent-accept">
						<i class="bi bi-check2 me-1"></i>Register and open
					</button>
					<button type="button" class="btn btn-outline-secondary" @click="cancel" data-testid="share-consent-cancel">Cancel</button>
				</div>
			</div>

			<div v-else-if="phase === 'registering' || phase === 'navigating' || phase === 'known'" class="text-muted">
				<span class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
				Opening shared view…
			</div>
		</div>
	`,
};
