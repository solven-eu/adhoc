import { ref, computed } from "vue";

// Preferences modal exposing the `?cdn` / `?dev` URL flags as user-facing toggles.
//
// Both flags are applied by the bootstrap block at the top of `index.html` — they drive
// which importmap JSON and which stylesheet variants the SPA loads. They are URL-scoped
// (not persisted) so a refresh without the query param reverts to the default, which is
// exactly the behaviour we want for an "escape hatch"-style switch.
//
// Applying a change here updates `location.search` and reloads the page. A soft swap is
// not possible: module graphs in ES modules are frozen after the importmap is applied, so
// a fresh document is required to pick up the new set of URLs.
//
// The Theme toggle is a stub for now — it reads/writes `data-bs-theme` on <html> so the
// Bootstrap 5.3+ colour scheme switch takes effect immediately, without reload. Persisted
// in localStorage because, unlike the asset flags, the user likely wants it to stick.
export default {
	setup() {
		const params = new URLSearchParams(location.search);
		const useCdn = ref(params.has("cdn"));
		const useDev = ref(params.has("dev"));
		const currentResourceMode = computed(() => (useCdn.value ? "cdn" : "webjars"));

		const BS_THEME_KEY = "adhoc.bsTheme";
		const theme = ref(localStorage.getItem(BS_THEME_KEY) || "auto");
		// Apply once at construction so a returning user sees their saved theme immediately.
		document.documentElement.setAttribute("data-bs-theme", theme.value);

		const applyAssetFlags = function () {
			// Build a fresh query string from the current toggle state. We intentionally drop
			// any other existing params — today this modal owns the full set of flags, and
			// preserving unknown params risks silently re-activating something the user thought
			// they had turned off.
			const next = new URLSearchParams();
			if (useCdn.value) next.set("cdn", "");
			if (useDev.value) next.set("dev", "");
			const qs = next.toString();
			// URLSearchParams.toString() renders `set("foo", "")` as `foo=`. We prefer the
			// value-less `?foo` form — both are accepted by `.has()` but the bare form is
			// idiomatic for on/off flags and matches how the user would type them.
			const clean = qs.replace(/=(&|$)/g, "$1");
			location.search = clean;
		};

		const applyTheme = function (nextTheme) {
			theme.value = nextTheme;
			document.documentElement.setAttribute("data-bs-theme", nextTheme);
			localStorage.setItem(BS_THEME_KEY, nextTheme);
		};

		return { useCdn, useDev, theme, currentResourceMode, applyAssetFlags, applyTheme };
	},
	template: /* HTML */ `
		<div class="modal fade" id="preferencesModal" tabindex="-1" aria-labelledby="preferencesModalLabel" aria-hidden="true">
			<div class="modal-dialog">
				<div class="modal-content">
					<div class="modal-header">
						<h5 class="modal-title" id="preferencesModalLabel"><i class="bi bi-sliders me-2"></i>Preferences</h5>
						<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
					</div>
					<div class="modal-body">
						<h6 class="text-muted text-uppercase small">Assets</h6>
						<p class="small text-muted">
							Changes in this block take effect after a page reload. Active mode is currently
							<strong>{{currentResourceMode}}</strong>.
						</p>
						<div class="form-check form-switch mb-2">
							<input class="form-check-input" type="checkbox" id="prefCdn" v-model="useCdn" />
							<label class="form-check-label" for="prefCdn">
								Use CDN
								<span class="text-muted small d-block">Load libraries from jsdelivr instead of the local WebJars.</span>
							</label>
						</div>
						<div class="form-check form-switch mb-3">
							<input class="form-check-input" type="checkbox" id="prefDev" v-model="useDev" />
							<label class="form-check-label" for="prefDev">
								Dev mode (full / source-readable builds)
								<span class="text-muted small d-block">Off by default — minified builds are served. Turn on for DevTools work.</span>
							</label>
						</div>
						<button type="button" class="btn btn-primary btn-sm" @click="applyAssetFlags">
							<i class="bi bi-arrow-clockwise me-1"></i>Apply &amp; reload
						</button>

						<hr class="my-4" />

						<h6 class="text-muted text-uppercase small">Theme</h6>
						<p class="small text-muted">Applied immediately; remembered across sessions (localStorage).</p>
						<div class="btn-group" role="group" aria-label="Theme selection">
							<button type="button" class="btn btn-outline-secondary btn-sm" :class="{active: theme === 'light'}" @click="applyTheme('light')">
								<i class="bi bi-sun me-1"></i>Light
							</button>
							<button type="button" class="btn btn-outline-secondary btn-sm" :class="{active: theme === 'dark'}" @click="applyTheme('dark')">
								<i class="bi bi-moon-stars me-1"></i>Dark
							</button>
							<button type="button" class="btn btn-outline-secondary btn-sm" :class="{active: theme === 'auto'}" @click="applyTheme('auto')">
								<i class="bi bi-circle-half me-1"></i>Auto
							</button>
						</div>
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-secondary btn-sm" data-bs-dismiss="modal">Close</button>
					</div>
				</div>
			</div>
		</div>
	`,
};
