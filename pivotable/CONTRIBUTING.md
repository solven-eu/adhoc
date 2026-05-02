# Contributing to Pivotable

This document is Pivotable-specific. For repository-wide rules see the root `CLAUDE.md`
(`mvn`, `spotless`, conventions). Anything below is the contract for working on the
Pivotable backend (`pivotable/server-{webflux,webmvc}`) and SPA (`pivotable/js`).

## Running the dev stack

The dev stack is a Spring Boot backend on `:8080` plus a Vite frontend dev server on
`:5173` that proxies `/api`, `/webjars`, `/login`, `/logout`, `/oauth2` to the backend.
All commands are run from `pivotable/js`:

|        Command         |                                                                      What it starts                                                                       |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `npm run backend`      | Spring Boot only (`mvn spring-boot:run`, profile `pivotable-unsafe` by default). Honours `$WEBMODE` (`webflux` / `webmvc`) and `$SPRING_ACTIVE_PROFILES`. |
| `npm run dev_frontend` | Vite dev server only — HMR on top of an already-running backend.                                                                                          |
| `npm run dev_stack`    | Both. Ctrl-C kills both.                                                                                                                                  |

`npx playwright test` (e2e) implicitly invokes `npm run backend` if nothing is on `:8080`.

## Logging in (default profile is `pivotable-unsafe_fakeuser`)

The default profile registers a hardcoded BASIC account whose username + password are
**pre-filled** in the login form, so a 3-click sequence is enough to reach the
authenticated SPA:

1. Home page → click **You need to login**.
2. Click the provider **pivotable-unsafe_fakeuser**.
3. Click **Login fakeUser**. (Username and password are already populated.)

Credentials, in case you need to drive the BASIC endpoint directly (curl, Playwright
non-UI flow, etc.):

```text
username: 11111111-1111-1111-1111-000000000000   # FakeUser.ACCOUNT_ID
password: no_password
```

Source of truth: `pivotable/server-webflux/src/main/java/eu/solven/adhoc/pivotable/webflux/security/PivotableSocialWebfluxSecurity.java`
(method `configureBasicForFakeUser`) and
`pivotable/js/src/main/resources/static/ui/js/login-basic.js` (lines that set
`username` / `password` refs to the same constants).

There is **no reason** to claim "I can't verify because I'm logged out" — this flow
works in the dev stack out of the box, and Playwright e2e tests exercise it (see
`pivotable/js/e2e-tests/query-pivotable.mjs`'s `login` and `queryPivotable` helpers).

## SPA URL flags (client-side, no server-side template)

The SPA loads its dependencies from one of four pre-built importmaps depending on two
URL query flags. **All resolution is client-side** — the Spring controller serves the
static `index.html` byte-for-byte; the inline bootstrap block in `<head>` reads the
flags and picks the right map.

|           URL           |            Importmap             |                                         Use when                                          |
|-------------------------|----------------------------------|-------------------------------------------------------------------------------------------|
| `/` (no flags, default) | `/ui/importmap-webjars-min.json` | Normal end-user load: minified, self-hosted via Spring Boot's `/webjars/*`. Air-gap-safe. |
| `/?dev`                 | `/ui/importmap-webjars.json`     | DevTools work — full / source-readable builds.                                            |
| `/?cdn`                 | `/ui/importmap-cdn-min.json`     | Verifying a fresh checkout before the backend is built; the local WebJar cache is stale.  |
| `/?cdn&dev`             | `/ui/importmap-cdn.json`         | Both at once.                                                                             |

Operators wanting minified bundles in a non-default deploy must include `?` (the
default already gives min) or, for explicit unminified, pass `?dev`. There is no
server-side `prdmode` HTML rewriting any more (that was deleted in 0.0.19).

## Vite dev mode caveat

In dev, Vite's `importmapExternalsPlugin` (`pivotable/js/vite.config.js`) hijacks every
bare specifier (`import "vue"`, `import "pinia"`) and rewrites it to an absolute
`/webjars/*` URL. The browser-level importmap is therefore *only* used by the inline
`<script type="module">` in `index.html`. To keep both resolution paths aligned, the
bootstrap **forces the webjars-min importmap in dev mode**, regardless of `?cdn`. The
`?cdn` and `?dev` flags only take effect under a non-Vite production-style serve. See
the comment in `index.html`'s bootstrap script for the full reasoning.

## Async UX rule (mirrored in `CLAUDE.md`)

Every async operation that the user is waiting on MUST surface in-flight state:

- a Bootstrap spinner (`spinner-border` / `spinner-grow`) **and** a short text status
  (`Loading providers…`, `Refreshing token…`); message-only is acceptable but
  spinner+message is the standard.
- the triggering control is `disabled` while the operation is in-flight (or the handler
  is debounced) — clicking a button that is already running must not re-fire.
- look for an existing sibling component using a loading pattern and reuse it rather
  than inventing a new one. The grid's progress bar is the canonical long-operation
  example; `spinner-border-sm` next to a label is the canonical short-operation one.

## File layout

```
pivotable/
├── infra/                Shared profile constants, model classes used by both servers.
├── server-webflux/       WebFlux/Netty entry point — preferred for new development.
├── server-webmvc/        Servlet/Tomcat entry point — kept for parity, but webflux is the canonical surface.
├── server-oauth2/        OAuth2 bridge (token service, JWT signing, /api/login/v1/*).
├── server-core/          Cross-cutting glue (Pivotable registry, examples, configs).
├── model/                DTOs shared between client and server (FakeUser, RandomUser, …).
├── server/               Standalone "all-in-one" launcher (used by Heroku).
└── js/                   SPA: Vue 3 + Pinia + SlickGrid + Bootstrap. See pivotable/js/CONTRIBUTING.md (TODO).
```

## Tests

|          Layer          |                      Where                       |                                           Run with                                            |
|-------------------------|--------------------------------------------------|-----------------------------------------------------------------------------------------------|
| Java unit / integration | `pivotable/server-*/src/test/java/**`            | `mvn test -Pnostyle --file pom.xml --batch-mode -pl pivotable/server-webflux` (or `-webmvc`). |
| JS unit (Vitest)        | `pivotable/js/unit-tests/*.spec.js`              | `cd pivotable/js && npx vitest run` (no DOM env needed today; Vitest defaults to node).       |
| E2E (Playwright)        | `pivotable/js/e2e-tests/localhost8080-*.spec.js` | `cd pivotable/js && npx playwright test`. Auto-starts the backend on `:8080`.                 |

Coverage:

- JS: `cd pivotable/js && npm run coverage_unit` (vitest+v8) and `npm run coverage_e2e`
  (Playwright via Istanbul) merge into `pivotable/js/coverage/`.
- Java: standard JaCoCo via `mvn verify`.

## Code review reminders specific to Pivotable

- `column.name` in the grid is **HTML** (it embeds the inline copy-name icon). Always
  key off `column.id` for model lookups, footer logic, and action callbacks. See the
  `headerNameWithCopyIcon` helper.
- Vue templates are JS template literals — backticks anywhere inside the template
  (including HTML comments) terminate the string and produce a confusing biome error.
  Use plain prose or single quotes. The same trap fires for `${...}` interpolations.
- The SPA never assumes the importmap is registered before code runs in dev — Vite
  rewrites the imports server-side. In prod it does. Don't tie new code to the
  presence of a particular `<script type="importmap">` in the DOM.

