---

description: Monthly whole-repo security audit tailored to the adhoc codebase (OAuth2 dual-stack, query engine, Vue frontend). Diff-scoped review belongs in /security-review; this sweep catches drift the per-PR review misses.
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Perform a monthly security sweep of the adhoc repository on the current branch.
Produce a single report grouped by severity (Critical / High / Medium / Low / Informational).
For each finding: file:line, what's wrong, exploit scenario, concrete fix. Do NOT edit code — report only.

## 1. Unsafe-profile leakage

The `pivotable-unsafe*` Spring profiles disable security for dev. Verify:

- No `application.yml` default (i.e. no profile) silently inherits unsafe values (signing keys,
  CORS `*`, permitAll routes, fake users).
- `PivotableSecurityWebnoneSpringConfig`-style guards that throw on unsafe-in-prod still fire for
  every unsafe profile — grep all `application-pivotable-unsafe*.yml` and confirm each is gated.
- No hard-coded JWT signing key, client secret, or OAuth2 public-credential leaks into a
  non-unsafe profile or into `pivotable-config.yml` defaults.

## 2. Dual WebFlux / WebMVC divergence

`server-webflux` and `server-webmvc` are parallel stacks. For every route/filter defined in one,
confirm the other has an equivalent. Specifically compare:

- `PivotableResourceServerWebfluxConfiguration` vs `PivotableResourceServerWebmvcConfiguration`
  (authorization rules, CSRF, CORS, session policy).
- Any `SecurityWebFilterChain` / `SecurityFilterChain` beans — same matchers, same `authenticated()`
  vs `permitAll()` decisions.

Report any route that is locked down in one stack but open in the other.

## 3. Endpoint authorization matrix

Enumerate every `@RestController`, `@Controller`, `RouterFunction`, `@GetMapping`/`@PostMapping`
under `pivotable/` and list:

- path, HTTP method, required authority/role, returns-sensitive-data? (y/n), accepts-user-query? (y/n).

Flag any endpoint that (a) has no explicit authorization, (b) accepts user input used in a query
or filter, (c) returns data crossing a tenant/account boundary without an account check.
Pay special attention to `PivotableAsynchronousQueriesManager`, `AdhocCubesRegistry`,
`PivotableEndpointsRegistry`, and the MCP module.

## 4. Query-engine injection surface

The adhoc engine evaluates user-supplied filters, aggregators, and expressions. Audit:

- Any code path where a `String` from an HTTP request is compiled/parsed into a filter, measure,
  or expression (JEXL/SpEL/MVEL/custom). Confirm an allow-list or sandboxed evaluator is used —
  never `Expression#getValue` on user input with a full `EvaluationContext`.
- SQL/JDBC table adapters (DuckDB, Sql, etc.): confirm parameter binding, not string concat.
- Deserialization: Jackson `@JsonTypeInfo` / polymorphic types — any `Object`/`@class`-driven
  deserialization on a request body is a red flag.

## 5. Multi-tenancy & account isolation

`PivotableUsersRegistry`, `IAdhocUserRepository`, and cube/endpoint registries hold
per-account data. Trace: given a request with account A's token, is there any path that returns
account B's cubes/endpoints/queries/results? Check `InMemoryUserRepository` lookups, async query
result retrieval (`QueryResultHolder` by query-id — is the account checked on fetch?), and
cancellation (can A cancel B's query?).

## 6. DoS / resource exhaustion

- `PivotableAsynchronousQueriesManager`: is the number of concurrent/queued queries per account
  bounded? Is memory per query bounded? Is there a query timeout enforced server-side?
- Any unbounded `Stream`/`List`/`Map` built from request data without a size cap.
- Webjar / static resource serving: confirm no path-traversal (`../`) reachable, size caps in place.

## 7. Frontend (pivotable/js)

- Any `v-html` / `innerHTML` fed by server response — XSS risk if the server ever returns
  attacker-controlled strings.
- `fetch`/axios calls: are credentials + CSRF tokens handled consistently?
- Storage of tokens: `localStorage` vs `httpOnly` cookie — flag any access token in
  `localStorage`/`sessionStorage`.
- Vite dev-proxy (`/api`, `/login`, `/logout`, `/webjars`) — dev-only config only, not in prod bundle.

## 8. Secrets & config

- Grep the working tree (not just `src/`) for likely secrets: `-----BEGIN`, `password:`,
  `secret:`, `api_key`, `AKIA`, `AIza`, long base64 strings in YAML. Exclude `target/` and
  `node_modules/`.
- `.env*`, `application-*.yml`, and any `*.properties` checked into git — confirm no real
  credentials, only dev/unsafe placeholders.

## 9. Dependencies (read-only, no pom edits)

Run `mvn -Pfast dependency:tree -DoutputType=text --batch-mode` (scoped to pivotable modules is
fine) and, if available, `npm --prefix pivotable/js audit --json`. Report:

- Any dependency with a known-critical CVE in the last 6 months.
- Transitive Logback / Jackson / Spring versions if they lag several minors behind.

Do NOT modify pom.xml — report findings only.

## 10. Crypto & tokens

- JWT: confirm `RS256`/`ES256`-class signing in prod, never `HS256` with a short shared secret,
  never `alg:none` accepted. Check `PivotableTokenService` and `ActiveRefreshTokens` for token
  lifetime, rotation, and refresh-token reuse detection.
- Any `MessageDigest.getInstance("MD5"|"SHA-1")` used for anything other than cache keys / ETags.
- `SecureRandom` used for token/id generation, not `Math.random()` or `Random`.

## 11. Logging & observability

- Grep for `log.*(user|token|password|secret|authorization|cookie)` — any call that could
  dump credentials into logs.
- Confirm structured errors returned to clients do not echo stack traces or SQL in prod.

## Output format

End the report with:

- A ranked top-5 "fix these first" list.
- A one-line delta note vs the previous sweep (user will paste prior report if desired).
- Any section where you could not reach a conclusion and what evidence would resolve it.

