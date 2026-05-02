# Pivotable

`Pivotable` is a library providing an API layer and a Single-Page-Application on top of Adhoc.

While it can be considered as an autonomous/full-fledged application, it remains a customizable library.

To rely on it through Spring, one should have the following configuration in your own configuration:
- `Environment`: `SPRING_CONFIG_IMPORT=classpath:pivotable-config.yml`
- `Property`: `-Dspring.config.import=classpath:pivotable-config.yml`
- `application.yml`: `spring.config.import: classpath:pivotable-config.yml`
- `application.properties`: `spring.config.import=classpath:pivotable-config.yml`

This is not provided by default as we keep Pivotable behaving as a library, and Spring-Boot required this property to be in the root configuration.

## Quick-Start

Run the main in:

    PivotableServerApplication

## Pivotable vs Adhoc

While: 
- Adhoc is a query-engine, knowing how to execute queries to return views
- Pivotable is a client, managing a set of Adhoc endpoints, and providing an example JS application to show a view as a grid
- Pivotable can embed an Adhoc endpoint (named `self`), or query remote Adhoc endpoints.

## Main concepts

*Pivotable* is a web application composed of a back-end and a front-end.

The back-end:

- Serves API for the front-end.

The front-end:

- is a Single-Page-Application coded in Vue3

Pivotable should generally be considered as a reference implementation, and not as the privileged client for Adhoc queries.

## Migration

Being a reference implementation, we focus:

- more into a lean implementation, making it easier to understand existing concepts
- less into a retro-compatible application, making migrations a smooth-less effort.

## APIs core concepts

Pivotable enables:

- managing a set of *Adhoc Endpoints*. **To be defined**
- querying *Endpoints* schema, *Endpoints Cubes schemas* and *Endpoints Cubes Columns*.
- Endpoints are identified by a UUID. They also have a name and a URL.
- Cubes are identified by a human-friendly String.

## SPA core concepts

Pivotable enables:

- Listing entrypoints
- Showing entrypoints schemas, especially the available cubes
- Showing cubes schemas, especially the available columns and measures
- Search through columns available members

The default implementations is based over:

- Vue3 as a module
- Does not require compilation
- Enable quick developments
- Make not be satisfactory for production application (as it is sub-optimal by not being compiled)
- Make not be satisfactory for a full-fledged application (as it has strong restrictions in term of available tools (no TypeScript, no `.vue` files, etc)).

## Architecture — static resources, no build step

Pivotable's SPA runs **as-is from the Spring Boot server**: no Webpack, no Rollup, no Babel, no TypeScript transpilation. The browser directly loads ES modules (`.js` / `.mjs`) from:

1. **Authored source** — the `*.js` Vue components under `pivotable/js/src/main/resources/static/ui/js/`, loaded by the `<script type="module">` block in `index.html` via native `import` statements.
2. **Third-party libraries** (Vue, vue-router, pinia, Bootstrap, SlickGrid, Mermaid, popperjs, sortablejs, lodash-es, bootstrap-icons, …) — loaded via a browser **import map** at the top of `index.html`.

This design is intentional: no build step means editing a `.js` file and refreshing the browser is the whole dev loop. The price is that the SPA cannot use TypeScript or `.vue` Single-File Components — only plain `.js` with template-literal render functions. See the bullet list above for the tradeoff call.

### WebJars vs CDN

The import map has two sources for every third-party library. A synchronous bootstrap `<script>` in `<head>` picks between them at page load:

| Mode | URL shape | Default? | When to use |
|------|-----------|----------|-------------|
| **WebJars** (local) | `/webjars/<artifact>/<version>/<path>` | ✅ yes | Production, air-gapped, enterprise networks |
| **CDN** (jsdelivr) | `https://cdn.jsdelivr.net/webjars/<groupId>/<artifact>/<version>/<path>` | via `?cdn` | Local dev without a built backend, quick verification of a fresh checkout |

Both paths resolve to the **same file from the same WebJar** — jsdelivr serves Maven Central artefacts, so the CDN and local modes return byte-identical content. Only the hostname differs.

**How to switch:**
- Default (no query param): `http://host/` → loads from `/webjars/*`.
- CDN override: `http://host/?cdn` → loads from jsdelivr.

**How to check which mode is active:** the bootstrap script exposes the chosen mode as `window.__adhocResourceMode` — inspect it from browser DevTools: `"webjars"` or `"cdn"`. It's also visible in the Network tab: check the hostname of the first `vue.esm-browser.js` request.

**Where the sources are wired:**
- Library versions: `pivotable/js/pom.xml` (each `org.webjars*` dependency).
- Runtime import map: the `IMPORTS` and `STYLESHEETS` tables inside the bootstrap `<script>` in `pivotable/js/src/main/resources/static/index.html`.
- Vite dev (`:5173`) externalisation: `IMPORTMAP_ALIASES` in `pivotable/js/vite.config.js` — mirrors the CDN arm so the Vite dev server points the browser at jsdelivr rather than `node_modules`.

The versions in `pom.xml` must match the versions spelled out in the CDN URLs (the URL path embeds the version). A regression test (`pivotable/js/unit-tests/index-html-default-mode.spec.js`) asserts the bootstrap defaults to WebJars — flip the test if you ever legitimately want to change the default.

See also: `SECURITY.MD § Pivotable index.html — static resource source` for the security rationale.

# Authentication

Pivotable authentication relies on a custom JWT. The JWT is generated following a login flow.

The signature of JWTs is managed by `PivotableResourceServerConfiguration`.
The generation of JWTs is managed by `PivotableTokenService`.

## Custom Authentication

The main class to define authentication methods is `PivotableLoginController`.

Pivotable provides 2 authentication methods by default:
- BASIC, through the Spring profile `pivotable-unsafe_fakeuser`
- OAuth2, with examples provided for Google and Github, relying on core [SpringBoot](https://docs.spring.io/spring-security/reference/servlet/oauth2/login/core.html).

One can register an additional oauth2 method with:

```
spring:
    application.name: pivotable-server
    security:
        oauth2:
            client:
                registration:
                    some_provider_id:
                        clientId: someClientId
                        clientSecret: someClientSecret
```