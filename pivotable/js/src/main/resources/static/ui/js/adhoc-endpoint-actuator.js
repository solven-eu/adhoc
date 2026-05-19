// @ts-check
import { ref, computed, onMounted } from "vue";

// Surfaces a small status block for a Pivotable endpoint, sourced from its Spring Boot Actuator
// endpoints (`/actuator/health` for UP/DOWN, `/actuator/info` for git+build metadata).
//
// Two clickable links to the raw `info` / `health` JSON are always rendered (open in a new tab),
// so the user can drill in even when the SPA can't fetch them itself.
//
// The fetch is best-effort: it uses `credentials: include` so same-origin endpoints (the `:self`
// sentinel) work seamlessly, but cross-origin endpoints almost always fail due to CORS — we
// silently render just the links in that case rather than surfacing a noisy error.
export default {
	props: {
		// Endpoint URL as carried by the adhoc store, e.g. `http://localhost:self` for the local
		// backend or `http://host:port[/prefix]` for a remote one. The `:self` sentinel is resolved
		// to relative URLs so the SPA hits its own origin.
		endpointUrl: {
			type: String,
			required: true,
		},
	},
	setup(props) {
		// Resolve the absolute actuator URLs. The `:self` sentinel becomes a relative path so the
		// browser uses the SPA's own origin (which IS the backend). Any other URL is treated as
		// absolute and the actuator paths are appended verbatim.
		const isSelf = computed(() => props.endpointUrl === "http://localhost:self");
		const infoUrl = computed(() => (isSelf.value ? "/actuator/info" : props.endpointUrl + "/actuator/info"));
		const healthUrl = computed(() => (isSelf.value ? "/actuator/health" : props.endpointUrl + "/actuator/health"));
		const threadDumpUrl = computed(() => (isSelf.value ? "/actuator/threaddump" : props.endpointUrl + "/actuator/threaddump"));

		/** @type {import("vue").Ref<"loading" | "ok" | "error">} */
		const fetchState = ref("loading");
		const status = ref("");
		const gitCommitId = ref("");
		const gitCommitTime = ref("");
		const buildTime = ref("");

		const tryFetch = async function (url) {
			try {
				const res = await fetch(url, { credentials: "include" });
				if (!res.ok) return null;
				return await res.json();
			} catch (e) {
				return null;
			}
		};

		onMounted(async () => {
			const [health, info] = await Promise.all([tryFetch(healthUrl.value), tryFetch(infoUrl.value)]);
			if (!health && !info) {
				fetchState.value = "error";
				return;
			}
			if (health && health.status) status.value = String(health.status);
			if (info && info.git && info.git.commit) {
				gitCommitId.value = info.git.commit.id ? String(info.git.commit.id) : "";
				gitCommitTime.value = info.git.commit.time ? String(info.git.commit.time) : "";
			}
			if (info && info.build && info.build.time) {
				buildTime.value = String(info.build.time);
			}
			fetchState.value = "ok";
		});

		// Thread-dump fetch: hits /actuator/threaddump with `Accept: text/plain` so Spring
		// Boot's content negotiation returns the classic jstack-style text dump (rather than
		// the default vnd.spring-boot.actuator.v3+json shape). The route is NOT under the
		// /api/v1 JWT chain — it's protected by the Login Realm (session cookie), the same
		// auth that fronts /html/** and friends. So we use a plain fetch with
		// `credentials: "include"` to ride the session cookie; the dedicated
		// `userStore.authenticatedFetch` is the wrong tool here (it prepends /api/v1 and
		// expects the bearer-token chain). On success we open the text in a new tab via a
		// Blob URL — keeps the click feeling like a target="_blank" anchor while letting us
		// inject the Accept header (which a plain anchor cannot do).
		/** @type {import("vue").Ref<"idle"|"loading"|"error">} */
		const threadDumpState = ref("idle");
		const openThreadDump = async () => {
			if (threadDumpState.value === "loading") return;
			threadDumpState.value = "loading";
			try {
				const response = await fetch(threadDumpUrl.value, {
					credentials: "include",
					headers: { Accept: "text/plain" },
				});
				if (!response.ok) {
					console.warn("Thread dump fetch failed", response.status, response.statusText);
					threadDumpState.value = "error";
					setTimeout(() => {
						if (threadDumpState.value === "error") threadDumpState.value = "idle";
					}, 2500);
					return;
				}
				const text = await response.text();
				const blob = new Blob([text], { type: "text/plain;charset=utf-8" });
				const blobUrl = URL.createObjectURL(blob);
				const newWindow = window.open(blobUrl, "_blank");
				if (!newWindow) {
					console.warn("Thread dump: window.open returned null (popup blocked?)");
				}
				// Best-effort cleanup — the blob URL stays alive long enough for the new tab
				// to start fetching; revoking after a minute keeps the in-memory leak bounded
				// when the user opens many dumps.
				setTimeout(() => URL.revokeObjectURL(blobUrl), 60_000);
				threadDumpState.value = "idle";
			} catch (e) {
				console.error("Thread dump fetch threw", e);
				threadDumpState.value = "error";
				setTimeout(() => {
					if (threadDumpState.value === "error") threadDumpState.value = "idle";
				}, 2500);
			}
		};

		return {
			fetchState,
			status,
			gitCommitId,
			gitCommitTime,
			buildTime,
			infoUrl,
			healthUrl,
			threadDumpUrl,
			threadDumpState,
			openThreadDump,
		};
	},
	template: /* HTML */ `
		<div class="small text-muted d-flex flex-wrap align-items-center gap-2 mt-1">
			<span v-if="fetchState === 'loading'">
				<span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>
				Probing actuator…
			</span>
			<span v-else-if="fetchState === 'ok' && status">
				<span class="badge" :class="status === 'UP' ? 'text-bg-success' : 'text-bg-danger'" data-testid="actuator-status">{{ status }}</span>
			</span>
			<span v-if="fetchState === 'ok' && gitCommitId">
				<i class="bi bi-git me-1"></i>
				<span class="font-monospace" data-testid="actuator-git-commit">{{ gitCommitId }}</span>
				<span v-if="gitCommitTime" class="ms-1 text-muted">({{ gitCommitTime }})</span>
			</span>
			<span v-if="fetchState === 'ok' && buildTime">
				<i class="bi bi-hammer me-1"></i>
				<span data-testid="actuator-build-time">{{ buildTime }}</span>
			</span>
			<span class="ms-auto">
				<a :href="infoUrl" target="_blank" rel="noopener" class="text-decoration-none me-2" data-testid="actuator-info-link">
					<i class="bi bi-info-circle me-1"></i>info
				</a>
				<a :href="healthUrl" target="_blank" rel="noopener" class="text-decoration-none me-2" data-testid="actuator-health-link">
					<i class="bi bi-heart-pulse me-1"></i>health
				</a>
				<!--
					Thread dump: a button (not an anchor) because the request needs an explicit
					Accept text/plain header to get the human-readable jstack-style output and
					an Authorization header to get past the JWT chain. Click opens the text in
					a new tab via a Blob URL — feels like a regular target="_blank" link.
				-->
				<button
					type="button"
					class="btn btn-link p-0 text-decoration-none align-baseline"
					@click="openThreadDump"
					:disabled="threadDumpState === 'loading'"
					:title="threadDumpState === 'error' ? 'Thread dump request failed — see the console' : 'Open a text/plain JVM thread dump in a new tab (requires login)'"
					data-testid="actuator-threaddump-link"
				>
					<span v-if="threadDumpState === 'loading'" class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>
					<i v-else-if="threadDumpState === 'error'" class="bi bi-exclamation-triangle text-danger me-1" aria-hidden="true"></i>
					<i v-else class="bi bi-bug me-1" aria-hidden="true"></i>
					thread dump
				</button>
			</span>
		</div>
	`,
};
