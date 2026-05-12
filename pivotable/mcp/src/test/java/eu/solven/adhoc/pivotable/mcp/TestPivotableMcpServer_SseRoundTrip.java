/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.pivotable.mcp;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.mcp.client.webflux.transport.WebFluxSseClientTransport;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.web.reactive.function.client.WebClient;

import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocEndpointMetadata;
import eu.solven.adhoc.pivotable.endpoint.PivotableEndpointsRegistry;
import eu.solven.adhoc.pivotable.endpoint.PivotableSchemaRegistry;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.ListToolsResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import tools.jackson.databind.ObjectMapper;

/**
 * Live end-to-end test for the Pivotable MCP server: boots a Spring Boot WebFlux app with the MCP server on a random
 * port, connects an in-process MCP client via SSE, performs the MCP {@code initialize} handshake, lists the exposed
 * tools, and invokes {@code listEndpoints} to verify a real client-server round-trip works over the wire. Unlike
 * {@link TestPivotableMcpTools} (which exercises the tool methods directly with mocks) and
 * {@link TestPivotableMcpConfiguration} (which only asserts bean wiring), this test exercises the SSE transport, the
 * JSON-RPC framing, and Spring AI's {@code @Tool} → MCP-protocol bridge.
 *
 * <p>
 * The {@link PivotableEndpointsRegistry} is pre-populated with one endpoint so {@code listEndpoints} returns a
 * deterministic, non-empty payload; the schema-side tools are exercised at the protocol level via
 * {@link TestPivotableMcpTools}.
 *
 * @author Benoit Lacelle
 */
@SpringBootTest(classes = TestPivotableMcpServer_SseRoundTrip.TestApp.class,
		webEnvironment = WebEnvironment.RANDOM_PORT,
		properties = { "spring.main.web-application-type=reactive",
				// Load the MCP server endpoints (/mcp/sse, /mcp/message) from the bundled YAML.
				"spring.profiles.active=pivotable-mcp" })
public class TestPivotableMcpServer_SseRoundTrip {

	// Minimal Spring Boot app: enables auto-config, imports the MCP configuration, and exposes a registry with one
	// pre-registered endpoint so listEndpoints has something to return.
	@SpringBootApplication
	@Import(PivotableMcpConfiguration.class)
	public static class TestApp {

		public static void main(String[] args) {
			SpringApplication.run(TestApp.class, args);
		}

		@Bean
		public PivotableEndpointsRegistry endpointsRegistry() {
			PivotableEndpointsRegistry registry = new PivotableEndpointsRegistry();
			registry.registerEntrypoint(PivotableAdhocEndpointMetadata.localhost());
			return registry;
		}

		@Bean
		public PivotableSchemaRegistry schemaRegistry() {
			// Empty schema registry: getSchema / executeQuery throw on the unknown UUID, which is fine — we test
			// listEndpoints over the wire here; happy-path schema / query tests live in TestPivotableMcpTools.
			return new PivotableSchemaRegistry();
		}

		@Bean
		public ObjectMapper jacksonObjectMapper() {
			return new ObjectMapper();
		}
	}

	@LocalServerPort
	int port;

	private McpSyncClient client;

	@BeforeEach
	void connectClient() {
		// Build the SSE transport pointing at our random-port server. Spring AI exposes the SSE endpoint at
		// /mcp/sse (set by application-pivotable-mcp.yml), with the message-back channel at /mcp/message handled
		// internally by the transport via the server-advertised endpoint event.
		WebFluxSseClientTransport transport =
				WebFluxSseClientTransport.builder(WebClient.builder().baseUrl("http://localhost:" + port))
						.sseEndpoint("/mcp/sse")
						.build();

		client = McpClient.sync(transport)
				// Generous timeouts: in CI under cold-start the SSE handshake can take a few seconds while Spring
				// finishes wiring.
				.initializationTimeout(Duration.ofSeconds(20))
				.requestTimeout(Duration.ofSeconds(10))
				.build();

		// MCP handshake — must succeed before any tool listing/invocation.
		McpSchema.InitializeResult initResult = client.initialize();
		assertThat(initResult).isNotNull();
		assertThat(initResult.serverInfo().name()).isEqualTo("pivotable-mcp-server");
	}

	@AfterEach
	void closeClient() {
		if (client != null) {
			client.closeGracefully();
		}
	}

	@Test
	public void listTools_exposesTheThreePivotableTools() {
		ListToolsResult tools = client.listTools();

		assertThat(tools.tools()).extracting(Tool::name).contains("listEndpoints", "getSchema", "executeQuery");

		// Spot-check that descriptions are non-empty so a future change that drops the @Tool description annotation
		// surfaces here — descriptions are what guide AI agents to pick the right tool.
		assertThat(tools.tools()).allSatisfy(t -> assertThat(t.description()).isNotBlank());
	}

	@Test
	public void callTool_listEndpoints_returnsRegisteredEndpoint() {
		CallToolResult result = client.callTool(new CallToolRequest("listEndpoints", Map.<String, Object>of()));

		assertThat(result.isError()).isNotEqualTo(Boolean.TRUE);

		// Tool returns a single string of "<uuid> <name>" lines — extract the text content and assert the
		// pre-registered localhost endpoint is reported back.
		String text = ((TextContent) result.content().get(0)).text();
		assertThat(text).contains(PivotableAdhocEndpointMetadata.SELF_ENTRYPOINT_ID.toString())
				.contains("localhost:self");
	}

	@Test
	public void callTool_getSchema_unknownEndpoint_surfacesError() {
		// No schema is registered for this UUID in our test app — the tool throws on the server side and the MCP
		// protocol surfaces it back to the client as an error result. Pins the error-propagation contract so a
		// future change that swallows server-side errors silently is caught here.
		CallToolResult result = client.callTool(
				new CallToolRequest("getSchema", Map.<String, Object>of("endpointId", UUID.randomUUID().toString())));

		assertThat(result.isError()).isEqualTo(Boolean.TRUE);
	}
}
