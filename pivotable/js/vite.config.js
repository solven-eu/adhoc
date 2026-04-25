import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";

// The SPA loads its dependencies via a browser import-map in `index.html` (production
// is the full switch — CDN vs local WebJars, see the `?cdn` and `?dev` modes). Vite's
// dev server needs to KNOW these bare specifiers so it doesn't try to resolve them
// from `node_modules` — we mirror the import-map here as a redirect table.
//
// We point at the LOCAL `/webjars/*` paths (proxied through Vite to the Spring Boot
// backend on `:8080`), and at the MINIFIED / `.prod.js` variants by default — same
// defaults the prod bootstrap uses. Two reasons:
//   1. The full vue-router 4.6.3 build trips a `__vrv_devtools` runtime error against
//      vue 3.5.x; the .prod.js build skips the devtools integration code path.
//   2. Keeping dev and prod on the same artefact paths means a bug reproducible in
//      dev is a bug reproducible in prod, and vice versa — no "works in dev only"
//      surprises.
// To run dev with full / source-readable builds (e.g. for breakpoint debugging in
// devtools), point this table at the non-`.prod.js` URLs temporarily.
//
// Keep in sync with `/ui/importmap-webjars-min.json` and `/ui/importmap-cdn-min.json`.
const IMPORTMAP_ALIASES = {
	vue: "/webjars/vue/3.5.32/dist/vue.esm-browser.prod.js",
	"vue-router": "/webjars/vue-router/4.6.3/dist/vue-router.esm-browser.prod.js",
	"@vue/devtools-api": "/webjars/vue__devtools-api/6.6.4/lib/esm/index.js",
	pinia: "/webjars/pinia/3.0.4/dist/pinia.esm-browser.js",
	"vue-demi": "/webjars/vue-demi/0.14.10/lib/v3/index.min.mjs",
	bootstrap: "/webjars/bootstrap/5.3.8/js/bootstrap.esm.min.js",
	"@popperjs/core": "/webjars/popperjs__core/2.11.8/dist/esm/index.js",
	slickgrid: "/webjars/slickgrid/5.18.2/dist/esm/index.mjs",
	sortablejs: "/webjars/sortablejs/1.15.7/modular/sortable.esm.js",
	lodashEs: "/webjars/lodash-es/4.17.21/lodash.js",
	mermaid: "/webjars/mermaid/11.6.0/dist/mermaid.esm.min.mjs",
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
					// Spring Security's OAuth2 authorization-request endpoints live at
					// `/oauth2/authorization/{provider}` (kicks off the external login flow) and
					// `/login/oauth2/code/{provider}` (callback — already covered by `/login`).
					// Without this entry, clicking a social-login provider returns Vite's
					// index.html instead of the 302 to github.com/google.com, and the SPA router
					// throws a no-route error on `/oauth2/authorization/github`.
					"/oauth2": backendProxy,
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
