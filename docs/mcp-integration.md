# MCP integration

Pivotable exposes its OLAP API to AI agents (Claude Code, custom MCP-aware clients) over the [Model Context
Protocol](https://modelcontextprotocol.io). The integration is shipped as the optional `pivotable-mcp` module —
include it on the classpath, activate the `pivotable-mcp` Spring profile, and three tools become reachable over
Server-Sent Events at `/mcp/sse`.

This article covers the **why**, the **wiring**, and the **testing** of that integration. It is aimed at a downstream
project consuming `pivotable-mcp` and at anyone curious how the bridge between Spring AI `@Tool` annotations and a
live MCP client works.

## What is exposed

Three Spring AI `@Tool` methods, designed to be invoked in order by an AI agent that wants to query a Pivotable cube
without being pre-wired to its schema:

| Order |                      Tool                       |                                Purpose                                |
|------:|-------------------------------------------------|-----------------------------------------------------------------------|
|     1 | `listEndpoints`                                 | List known OLAP endpoints (one line per endpoint: `<uuid> <name>`).   |
|     2 | `getSchema(endpointId)`                         | Discover an endpoint's cubes, measures and dimension columns as JSON. |
|     3 | `executeQuery(endpointId, cubeName, queryJson)` | Run a `CubeQuery` and return a tabular result as JSON.                |

Source: [`PivotableMcpTools`](https://github.com/solven-eu/adhoc/blob/master/pivotable/mcp/src/main/java/eu/solven/adhoc/pivotable/mcp/PivotableMcpTools.java).
The MCP framing (JSON-RPC over SSE, the `tools/list` and `tools/call` envelopes) is handled by Spring AI's
`spring-ai-starter-mcp-server-webflux` starter — Pivotable only declares the method signatures and annotations.

## Wiring

Two things must be in place: the dependency on the classpath, and the Spring profile activated.

**Maven** — pick the dependency matching your servlet stack:

```xml
<!-- WebFlux (reactive) server -->
<dependency>
	<groupId>eu.solven.adhoc</groupId>
	<artifactId>pivotable-mcp-webflux</artifactId>
	<version>${pivotable.version}</version>
</dependency>

<!-- OR: WebMVC (servlet) server -->
<dependency>
	<groupId>eu.solven.adhoc</groupId>
	<artifactId>pivotable-mcp-webmvc</artifactId>
	<version>${pivotable.version}</version>
</dependency>
```

The two starter modules each pull in `pivotable-mcp` (the transport-agnostic `@Tool` definitions) plus the matching
Spring AI server starter (`spring-ai-starter-mcp-server-webflux` or `spring-ai-starter-mcp-server-webmvc`).
Pivotable's own `pivotable-server-webflux` / `pivotable-server-webmvc` modules already declare the appropriate
starter, so an application building on top of either inherits the MCP wiring transitively.

**Spring `@Import`** — the MCP configuration class is gated on `@Profile("pivotable-mcp")`, but Spring still needs
to know about it. Add it to your application's `@Import` list (Pivotable's own server applications already do this):

```java
@SpringBootApplication(scanBasePackages = "none")
@Import({
	// … existing imports …
	PivotableMcpConfiguration.class,
})
public class MyApplication { ... }
```

Without this import the MCP beans are never seen — even with the profile active and the dependency on the
classpath. (Component-scan-based apps don't need the explicit `@Import`; Pivotable uses `scanBasePackages = "none"`
and relies on explicit `@Import` everywhere.)

**Profile**: activate `pivotable-mcp`. Either combined with other profiles in `SPRING_PROFILES_ACTIVE`, or via the
project's `application.yml`:

```yaml
spring:
profiles:
	active: pivotable-unsafe,pivotable-mcp
```

The profile loads [`application-pivotable-mcp.yml`](https://github.com/solven-eu/adhoc/blob/master/pivotable/mcp/src/main/resources/application-pivotable-mcp.yml)
which sets the SSE endpoint paths:

```yaml
spring:
ai:
	mcp:
	server:
		enabled: true
		name: pivotable-mcp-server
		sse-endpoint: /mcp/sse
		sse-message-endpoint: /mcp/message
```

That is the entire server-side wiring. Spring AI auto-discovers the `ToolCallbackProvider` bean declared in
[`PivotableMcpConfiguration`](https://github.com/solven-eu/adhoc/blob/master/pivotable/mcp/src/main/java/eu/solven/adhoc/pivotable/mcp/PivotableMcpConfiguration.java)
and routes `tools/list` / `tools/call` JSON-RPC calls to the corresponding `@Tool`-annotated methods on
`PivotableMcpTools`.

## Connecting a client

There are two practical client setups: an AI-agent CLI like Claude Code, and a programmatic Java client.

### Claude Code (interactive)

The `/mcp/**` routes are subject to the existing `PivotableJwtWebfluxSecurity` chain — the SPA's regular JWT
authenticates them too. **Get a Bearer token from the SPA:**

1. Log in to the SPA at `http://localhost:8080/` (e.g. via `pivotable-unsafe_fakeuser`).
2. Open the logged-in account chip (top-right "Welcome <user>"). Below the country selector, click
   **Copy MCP token** — it copies `Authorization: Bearer <jwt>` to the clipboard. Expand **Show MCP token** if
   you want to read it instead of relying on the clipboard.

Then add an entry to `.mcp.json` at the repo root (alongside any existing entries such as the `playwright` one):

```json
{
	"mcpServers": {
		"pivotable": {
			"type": "sse",
			"url": "http://localhost:8080/mcp/sse",
			"headers": { "Authorization": "Bearer <paste-the-token-here>" }
		}
	}
}
```

Restart the Claude Code session. The three tools become available as `mcp__pivotable__listEndpoints`,
`mcp__pivotable__getSchema`, `mcp__pivotable__executeQuery`. The backend must be running with the
`pivotable-mcp` profile active before the session starts — Claude Code does **not** spawn it.

JWTs are short-lived (the SPA auto-refreshes on its end). If your Claude Code session starts erroring with 401
mid-session, re-acquire a token via the same UI flow and update `.mcp.json`. A proper long-lived API-token flow
for AI agents is a [ROADMAP](https://github.com/solven-eu/adhoc/blob/master/ROADMAP.MD) item.

### Programmatic (Java)

Use the Spring AI MCP client SDK that already ships with the server starter
(`io.modelcontextprotocol.sdk:mcp-core` + `org.springframework.ai:mcp-spring-webflux`). The minimal client is:

```java
WebFluxSseClientTransport transport = WebFluxSseClientTransport
	.builder(WebClient.builder().baseUrl("http://localhost:8080"))
	.sseEndpoint("/mcp/sse")
	.build();

try (McpSyncClient client = McpClient.sync(transport)
		.initializationTimeout(Duration.ofSeconds(20))
		.requestTimeout(Duration.ofSeconds(10))
		.build()) {
	client.initialize();
	ListToolsResult tools = client.listTools();
	CallToolResult result = client.callTool(new CallToolRequest("listEndpoints", Map.of()));
	String text = ((TextContent) result.content().get(0)).text();
	// …
}
```

The same shape is used by [`TestPivotableMcpServer_SseRoundTrip`](https://github.com/solven-eu/adhoc/blob/master/pivotable/mcp/src/test/java/eu/solven/adhoc/pivotable/mcp/TestPivotableMcpServer_SseRoundTrip.java)
— see the next section.

## Testing the integration

Three layers of test coverage are provided, in increasing breadth:

1. **Logic-level**, with mocks:
   [`TestPivotableMcpTools`](https://github.com/solven-eu/adhoc/blob/master/pivotable/mcp/src/test/java/eu/solven/adhoc/pivotable/mcp/TestPivotableMcpTools.java).
   Asserts the tool methods' behaviour directly — no Spring context, no network.
2. **Bean-wiring**, with `@SpringBootTest`:
   [`TestPivotableMcpConfiguration`](https://github.com/solven-eu/adhoc/blob/master/pivotable/mcp/src/test/java/eu/solven/adhoc/pivotable/mcp/TestPivotableMcpConfiguration.java).
   Asserts that `PivotableMcpTools` and the `ToolCallbackProvider` end up in the Spring context when
   `PivotableMcpConfiguration` is imported.
3. **End-to-end protocol**, with a real SSE client:
   [`TestPivotableMcpServer_SseRoundTrip`](https://github.com/solven-eu/adhoc/blob/master/pivotable/mcp/src/test/java/eu/solven/adhoc/pivotable/mcp/TestPivotableMcpServer_SseRoundTrip.java).
   Boots a Spring Boot WebFlux app on a random port with the `pivotable-mcp` profile active, connects an in-process
   `McpSyncClient` via `WebFluxSseClientTransport`, performs the MCP `initialize` handshake, lists tools, and invokes
   them. **No AI agent required.** This is the test to extend when adding a new `@Tool` method — a bug in the
   annotation / JSON schema / error propagation surfaces here rather than at the first agent integration.

The third layer is the most expensive (full Spring Boot start ≈ 5 s) but the only one that catches regressions in the
SSE transport, JSON-RPC framing, and the Spring AI `@Tool` → MCP-protocol bridge.

## Operational checklist

When wiring this into a downstream project, walk through:

- [ ] `pivotable-mcp-webflux` (reactive) **or** `pivotable-mcp-webmvc` (servlet) Maven dependency is on the runtime
  classpath of the server module.
- [ ] `PivotableMcpConfiguration` is in the `@Import` list of the `@SpringBootApplication`.
- [ ] `pivotable-mcp` Spring profile is active (additive to existing profiles).
- [ ] At least one `PivotableAdhocEndpointMetadata` is registered in `PivotableEndpointsRegistry` at startup —
  otherwise `listEndpoints` returns an empty string and the agent has nothing to drill into.
- [ ] The matching `IAdhocSchema` is registered in `PivotableSchemaRegistry` keyed by the endpoint's UUID —
  otherwise `getSchema` / `executeQuery` throw at the first call.
- [ ] The SSE endpoint (`/mcp/sse` by default) is reachable from the client. With a reverse proxy, ensure SSE
  buffering is disabled (`proxy_buffering off;` for nginx).

If `listTools` returns the three tools but `callTool` errors, the issue is on the Pivotable side (missing endpoint /
schema registration) — not the MCP transport. Run `TestPivotableMcpServer_SseRoundTrip` against the same profile to
confirm.

## Generating a new access_token

The SPA's "Copy MCP token" button surfaces the **access_token** the SPA is currently using. Access tokens are
short-lived (15 min by default — see `PivotableTokenService`) — when an MCP client errors with 401 mid-session,
you need a fresh one. There are two ways to obtain it:

### Via the SPA (interactive)

1. Reload the SPA at `http://localhost:8080/` — if your session is still alive, this triggers the SPA's automatic
   refresh-token round-trip and you get a new access_token without re-logging-in.
2. Open the logged-in account chip (top-right "Welcome <user>") and click **Copy MCP token** again.
3. Paste the new value into `.mcp.json` (or whichever client config) and restart the client.

If the session has expired entirely, re-run the login flow from `pivotable-unsafe_fakeuser` (or your configured
provider) before step 1.

### Via the API (programmatic)

For longer-lived MCP automation, exchange a **refresh_token** for a fresh access_token by calling
`GET /api/v1/oauth2/token` with the refresh_token as the `Authorization: Bearer` header. The endpoint is implemented
by `AccessTokenController` (webmvc) / `AccessTokenHandler` (webflux) — both return an `AccessTokenWrapper` JSON
payload containing the new `access_token` and its expiry.

```bash
# 1. Obtain a refresh_token by logging in once (e.g. via the SPA), then read it from the SPA store.
# 2. Exchange it for a fresh access_token:
curl -s -H "Authorization: Bearer $REFRESH_TOKEN" \
	http://localhost:8080/api/v1/oauth2/token \
| jq -r '.access_token'
```

Refresh tokens are long-lived (1 year by default) and revocable via `ActiveRefreshTokens`. A proper API-token flow
designed for AI agents (no SPA round-trip, scoped to MCP-only) is a [ROADMAP](https://github.com/solven-eu/adhoc/blob/master/ROADMAP.MD)
item.

## Chat (Anthropic-powered query assistant)

Separate from MCP — and serving the **opposite direction** — the Pivotable SPA ships an in-browser **chat assistant**
that lets a user describe a query in natural language ("show me revenue by country last quarter") and have the
backend forward the conversation to the [Anthropic Messages API](https://docs.claude.com/en/api/messages), grounded
in the targeted cube's schema. The model can call three tools — `set_measures`, `set_groupby`, `clear_query` — whose
calls are streamed back as Server-Sent Events and applied client-side to the query-builder UI.

MCP and Chat are independent: MCP lets *external* AI clients drive Pivotable; Chat lets the SPA drive Anthropic.
You can enable either, both, or neither.

### Obtaining an Anthropic credential

You need a credential from Anthropic — there is no signup flow inside Pivotable. Two options, both covered in detail
in [SECRETS.MD](https://github.com/solven-eu/adhoc/blob/master/SECRETS.MD):

- **`claude setup-token`** (recommended for CI / GitHub Actions / users on a Claude Pro or Max subscription).
  Generates a `sk-ant-oat01-…` OAuth token locally — no copy-paste from the web console.
- **Web-console API key** (`sk-ant-api03-…`) — pay-per-token, billed to the API account. Create one at
  [console.anthropic.com](https://console.anthropic.com) under **API keys**. Copy it immediately; the console only
  shows it once.

> Caveat carried over from `SECRETS.MD`: an `oat01` token from a Max subscription used as a GitHub Actions secret
> only lives ~24h
> ([anthropics/claude-code-action#727](https://github.com/anthropics/claude-code-action/issues/727)). For long-lived
> automation, the API-key route is more practical today.

Pivotable's backend auto-detects the credential format from the prefix of
`ADHOC_PIVOTABLE_CHAT_ANTHROPIC_API_KEY` and picks the right Anthropic auth header:

- `sk-ant-api03-…` → `x-api-key: <key>` (legacy API-key path).
- `sk-ant-oat01-…` → `Authorization: Bearer <token>` (OAuth path used by Claude Code).

You can swap formats by changing only the env var; no code or config change.

> **Subscription billing for shared services is a grey area.** A Pivotable server hitting Anthropic with a personal
> Pro/Max subscription's `oat01` token — for chat turns initiated by anyone using the Pivotable UI — effectively
> means the subscription holder pays for everyone. For personal-laptop dev that is fine; for a shared or hosted
> Pivotable deployment, read Anthropic's terms of service first.
>
> **Never commit either credential.** The Pivotable backend forwards it to `api.anthropic.com` on every chat turn —
> leaked tokens get scraped from public Git history within minutes. If you accidentally commit one, revoke it in
> the console (or run `claude logout` for an oat01 token, then re-run `setup-token`) and create a new one. For
> shared dev environments, prefer a separate Anthropic workspace with its own spend limit over a personal key.

### Storing the key safely

Three patterns, ranked by leak-safety on a personal dev machine:

**1. macOS Keychain (recommended on macOS).** Stores the key encrypted at rest, scoped to your login. Add it once,
read it on demand:

```bash
# One-off: store via interactive prompt so the key never appears in shell history.
security add-generic-password -a "$USER" -s anthropic-pivotable -w

# Then in your ~/.zshrc (or inline before `npm run dev_stack`):
export ADHOC_PIVOTABLE_CHAT_ANTHROPIC_API_KEY="$(security find-generic-password -a "$USER" -s anthropic-pivotable -w)"
```

**2. Shell rc file.** Less secure (plaintext on disk) but portable across shells and Linux/macOS. Edit `~/.zshrc`
(or `~/.bashrc`) and add:

```bash
export ADHOC_PIVOTABLE_CHAT_ANTHROPIC_API_KEY="sk-ant-api03-…"
```

Then `chmod 600 ~/.zshrc` and `source ~/.zshrc`. The file lives in your home dir, never in the repo.

**3. Local `.env.local` next to the project.** Useful when multiple env vars accumulate. Create
`adhoc/.env.local`:

```bash
ADHOC_PIVOTABLE_CHAT_ANTHROPIC_API_KEY=sk-ant-api03-…
```

Confirm `.env.local` (and `.env`) are in `.gitignore` before saving the file. Load it just before starting the
backend:

```bash
set -a; source .env.local; set +a
cd pivotable/js && npm run dev_stack
```

**Anti-patterns** — none of these are safe, despite being convenient:

- ❌ Inlining the key in `application.yml` or any file under the repo tree. A pre-commit hook can miss it; once
  pushed, history is forever (revoking + rotating is the only fix).
- ❌ Typing `export ADHOC_…_API_KEY=sk-ant-…` directly into a terminal — the key lands in `~/.zsh_history` and
  syncs with cloud history settings on macOS.
- ❌ Storing the key in a chat / ticket / shared note. Treat it the same as a database password.

### Activation

All chat-related settings live under the `adhoc.pivotable.chat.*` property tree, bound to
[`PivotableChatProperties`](https://github.com/solven-eu/adhoc/blob/master/pivotable/server-core/src/main/java/eu/solven/adhoc/pivotable/chat/PivotableChatProperties.java)
on the backend. The chat endpoint activates only when `adhoc.pivotable.chat.anthropic-api-key` is set; otherwise the
configuration class is silently skipped (`@ConditionalOnProperty`) and the SPA's chatbot self-hides because
`GET /api/v1/cubes/chat/enabled` returns 404 instead of 204.

**Preferred: YAML** — easier to review, diffable in PRs, and structured. In your project's `application.yml`
(or under a profile such as `application-pivotable-unsafe.yml`):

```yaml
adhoc:
pivotable:
	chat:
	anthropic-api-key: ${ANTHROPIC_API_KEY}  # interpolate from the env so the key never lands in the repo
	model: claude-haiku-4-5                  # default; override with claude-sonnet-4-6 etc.
	force-tool-call: false                   # true → Anthropic must end every turn with a tool call
	style:
		max-sentences: 2                       # cap on the model's plain-text reply length
		ambiguity: BEST_GUESS                  # BEST_GUESS (default) or CLARIFY
```

Then export only the secret before starting:

```bash
export ANTHROPIC_API_KEY=sk-ant-…
cd pivotable/js && npm run dev_stack
```

**Env-var fallback** — Spring's
[relaxed binding](https://docs.spring.io/spring-boot/reference/features/external-config.html#features.external-config.typesafe-configuration-properties.relaxed-binding)
also accepts each setting as an env var of the same path with `.`/`-` replaced by `_`. Useful for container
deployments where shipping a YAML overlay is heavier than setting envs:

```bash
export ADHOC_PIVOTABLE_CHAT_ANTHROPIC_API_KEY=sk-ant-…
export ADHOC_PIVOTABLE_CHAT_MODEL=claude-sonnet-4-6
export ADHOC_PIVOTABLE_CHAT_STYLE_AMBIGUITY=CLARIFY
```

### What the SPA expects on the wire

- `GET /api/v1/cubes/chat/enabled` → `204 No Content` when chat is wired up (any other status hides the chatbot UI).
- `POST /api/v1/cubes/chat` (JSON body: `ChatRequest`) → `text/event-stream` of simplified events:
- `{"type":"text","content":"…"}` — text fragment to display
- `{"type":"tool_use","name":"set_measures","input":{…}}` — apply this tool call to the query builder
- `{"type":"done"}` — stream finished
- `{"type":"error","message":"…"}` — abort

The server-side parsing of Anthropic's verbose streaming protocol lives in `AnthropicSseTranslator`
(`pivotable-server-core`) and is identical between the WebFlux and WebMVC implementations. The wire contract above
is the public surface — internal Anthropic event shapes do not leak to the SPA.

### WebFlux vs WebMVC

Both server stacks expose the chat endpoint with the same contract:

|  Stack  |                            Wiring class                             |                                            Implementation                                             |
|---------|---------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| WebFlux | `eu.solven.adhoc.pivotable.webflux.chat.PivotableChatConfiguration` | `PivotableChatHandler` + Spring `WebClient` reactive SSE consumer                                     |
| WebMVC  | `eu.solven.adhoc.pivotable.webmvc.chat.PivotableChatConfiguration`  | `PivotableChatController` + JDK `HttpClient` + `SseEmitter` (pumped on the application task executor) |

Pick the server stack first (`WEBMODE=webflux` or `WEBMODE=webmvc` when starting the dev stack); the chat endpoint
becomes available on whichever one is running. There is no cross-stack interaction.

### Cost & safety reminders

- Every chat turn triggers an Anthropic API call — billed to the configured key. Do **not** commit the key.
- The model is given the cube's measures and dimension columns. It does **not** see any data — only schema names.
  Sensitive column *names* are still exposed; if that is a concern, gate access to the chat endpoint behind a role
  in your security configuration.
- The tools the model can call (`set_measures`, `set_groupby`, `clear_query`) only mutate **client-side UI state** —
  they do not execute queries against the cube. Query execution still goes through the regular query path
  (`PivotableQueryController` / `PivotableQueryHandler`) under the user's normal authorization.

