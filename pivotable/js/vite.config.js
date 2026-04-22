import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";

// The SPA loads its dependencies via a browser import-map in `index.html` (pinned
// CDN URLs + webjars). Vite's dev server needs to KNOW these bare specifiers so
// it doesn't try to resolve them from `node_modules` — we mirror the import-map
// as a redirect table below.
//
// In dev, the plugin rewrites every `import "vue-router"` to
// `import "https://unpkg.com/vue-router@…/…"` and tells Vite the resolved id is
// external (a URL the browser fetches natively).
//
// Keep in sync with `<script type="importmap">` in
// `src/main/resources/static/index.html`.
const IMPORTMAP_ALIASES = {
	vue: "https://unpkg.com/vue@3.5.29/dist/vue.esm-browser.js",
	"vue-router": "https://unpkg.com/vue-router@5.0.3/dist/vue-router.esm-browser.js",
	"@vue/devtools-api": "https://unpkg.com/@vue/devtools-api@6.2.1/lib/esm/index.js",
	pinia: "https://unpkg.com/pinia@3.0.4/dist/pinia.esm-browser.js",
	"vue-demi": "https://cdn.jsdelivr.net/npm/vue-demi/lib/v3/index.mjs",
	// `bootstrap` is served by a webjar in production (`/webjars/bootstrap/js/bootstrap.esm.js`).
	// Vite treats root-relative paths as local files (ignores `external: true`), so we point at a
	// CDN copy for dev to keep the URL absolute and fetched by the browser directly.
	bootstrap: "https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.esm.min.js",
	"@popperjs/core": "https://unpkg.com/@popperjs/core@2.11.8/dist/esm/index.js",
	slickgrid: "https://cdn.jsdelivr.net/npm/slickgrid@5.18.2/dist/esm/index.mjs",
	sortablejs: "https://cdn.jsdelivr.net/npm/sortablejs@1.15.7/modular/sortable.esm.js",
	lodashEs: "https://cdn.jsdelivr.net/npm/lodash-es@4.17.23/+esm",
	mermaid: "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.mjs",
};

/**
 * Inline plugin: redirect bare import-map specifiers to the URLs the browser
 * import-map would resolve them to. Marking `external: true` tells Vite to emit
 * the resolved URL as-is in the transformed module, and the browser fetches it
 * natively — no `node_modules`, no Vite pre-bundling.
 */
function importmapExternalsPlugin() {
	return {
		name: "importmap-externals",
		enforce: "pre",
		resolveId(source) {
			const target = IMPORTMAP_ALIASES[source];
			if (target) {
				return { id: target, external: true };
			}
			return null;
		},
	};
}

// Shared proxy target for every path that belongs to the Spring Boot backend.
// `changeOrigin` rewrites the outbound Host header to the backend (otherwise Netty
// sometimes bins requests whose Host does not match its binding). `xfwd` adds
// X-Forwarded-{For,Port,Proto}; we add X-Forwarded-Host manually since http-proxy
// omits it. With `server.forward-headers-strategy=framework` on the backend (enabled
// only under the `pivotable-unsafe` profile), Spring Security / OAuth2 then build
// redirects against `localhost:5173` instead of the proxied `127.0.0.1:8080`, so the
// browser stays on the Vite dev server across login flows.
const backendProxy = {
	target: "http://127.0.0.1:8080",
	changeOrigin: true,
	xfwd: true,
	configure: (proxy) => {
		proxy.on("proxyReq", (proxyReq, req) => {
			if (req.headers.host) {
				proxyReq.setHeader("X-Forwarded-Host", req.headers.host);
			}
		});
	},
};

// https://vitejs.dev/config/
export default defineConfig(({ command }) => {
	if (command === "serve") {
		// `npm run dev` — Vite dev server with HMR.
		//
		// Layout: `index.html` + the JS entry points live under
		// `src/main/resources/static/` (same layout Spring Boot ships for
		// production). `root` points Vite there so the usual `/` URL serves
		// the SPA.
		//
		// Proxy: the Spring Boot backend (run with e.g.
		// `mvn -f ../server-webflux/pom.xml spring-boot:run -Dspring-boot.run.profiles=pivotable-unsafe`)
		// owns `/api/*`, `/webjars/*`, `/login*`. Vite owns everything else
		// (`index.html`, `/ui/js/**`, `/favicon.ico`, `/html/*` which falls
		// back to `index.html` via the default SPA history-API behaviour).
		return {
			root: "src/main/resources/static",
			plugins: [importmapExternalsPlugin(), vue()],
			optimizeDeps: {
				// These are CDN-loaded at runtime — never pre-bundle from disk.
				exclude: Object.keys(IMPORTMAP_ALIASES),
			},
			server: {
				port: 5173,
				proxy: {
					"/api": backendProxy,
					"/webjars": backendProxy,
					"/login": backendProxy,
					// `/logout` is registered by Spring Security at the root (not under `/api`),
					// so it needs its own proxy entry — otherwise the POST falls through to the
					// Vite static handler, which returns index.html and breaks `await response.json()`.
					"/logout": backendProxy,
				},
			},
		};
	}
	// `npm run build` — production bundle. Unchanged historical behaviour: the
	// built assets are served under `/src/main/resources/static/ui` when shipped
	// as part of the Spring Boot jar.
	return {
		base: "/src/main/resources/static/ui",
		plugins: [vue()],
	};
});
