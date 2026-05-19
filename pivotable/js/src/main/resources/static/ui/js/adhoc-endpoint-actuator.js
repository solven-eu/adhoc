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

		return { fetchState, status, gitCommitId, gitCommitTime, buildTime, infoUrl, healthUrl };
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
				<a :href="healthUrl" target="_blank" rel="noopener" class="text-decoration-none" data-testid="actuator-health-link">
					<i class="bi bi-heart-pulse me-1"></i>health
				</a>
			</span>
		</div>
	`,
};
