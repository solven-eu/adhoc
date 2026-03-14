/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.pivotable.chat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.beta.schema.CubeSchemaMetadata;
import eu.solven.adhoc.beta.schema.EndpointSchemaMetadata;
import eu.solven.adhoc.beta.schema.IAdhocSchema;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocSchemaRegistry;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Handles POST {@code /cubes/chat}: proxies user messages to the Anthropic API with the cube schema as context, and
 * streams back simplified SSE events that the Vue chatbot can act on.
 *
 * <p>
 * Simplified SSE event types emitted to the client:
 * <ul>
 * <li>{@code {"type":"text","content":"..."}} — a text fragment to display</li>
 * <li>{@code {"type":"tool_use","name":"...","input":{...}}} — a tool call to apply to the query model</li>
 * <li>{@code {"type":"done"}} — stream finished</li>
 * <li>{@code {"type":"error","message":"..."}} — error</li>
 * </ul>
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class PivotableChatHandler {
	private static final int MAX_TOKENS = 1024;

	final PivotableAdhocSchemaRegistry schemasRegistry;
	final ObjectMapper objectMapper;
	final WebClient anthropicClient;
	final String model;

	public Mono<ServerResponse> chat(ServerRequest request) {
		return request.bodyToMono(ChatRequest.class).flatMap(chatRequest -> {
			AdhocSchema schema = schemasRegistry.getSchema(chatRequest.getEndpointId());

			IAdhocSchema.AdhocSchemaQuery schemaQuery =
					IAdhocSchema.AdhocSchemaQuery.builder().cube(Optional.of(chatRequest.getCube())).build();
			EndpointSchemaMetadata metadata = schema.getMetadata(schemaQuery, false);

			String systemPrompt = buildSystemPrompt(chatRequest.getCube(), metadata);

			List<Map<String, Object>> messages = buildMessages(chatRequest);

			Map<String, Object> anthropicBody = new LinkedHashMap<>();
			anthropicBody.put("model", model);
			anthropicBody.put("max_tokens", MAX_TOKENS);
			anthropicBody.put("stream", true);
			anthropicBody.put("system", systemPrompt);
			anthropicBody.put("messages", messages);
			anthropicBody.put("tools", buildTools());

			Flux<String> sseFlux = callAnthropic(anthropicBody);

			return ServerResponse.ok().contentType(MediaType.TEXT_EVENT_STREAM).body(sseFlux, String.class);
		});
	}

	protected String buildSystemPrompt(String cube, EndpointSchemaMetadata metadata) {
		StringBuilder sb = new StringBuilder();
		sb.append("You are a helpful data analyst assistant embedded in a query builder UI.\n");
		sb.append("The user is building a query against OLAP cube '").append(cube).append("'.\n\n");

		CubeSchemaMetadata cubeSchema = metadata.getCubes().get(cube);
		if (cubeSchema != null) {
			sb.append("Available measures (use exact names):\n");
			cubeSchema.getMeasures().keySet().stream().sorted().forEach(m -> sb.append("  - ").append(m).append("\n"));

			if (cubeSchema.getColumns() != null) {
				sb.append("\nAvailable dimension columns (use exact names):\n");
				cubeSchema.getColumns()
						.getColumns()
						.keySet()
						.stream()
						.sorted()
						.forEach(c -> sb.append("  - ").append(c).append("\n"));
			}
		}

		sb.append("""

				When the user asks to see or analyse data, call the appropriate tools:
				- set_measures: select which measures to display (e.g. "show revenue" → select Revenue.SUM)
				- set_groupby: set groupBy dimensions (e.g. "by country" → add Country column)
				- clear_query: reset all selections when the user wants to start over

				Rules:
				- Always use EXACT names from the schema above.
				- You may call multiple tools in one turn (e.g. set_measures AND set_groupby together).
				- Respond briefly in plain text, then call the tool(s).
				- If the request is ambiguous, ask for clarification before calling tools.
				""");

		return sb.toString();
	}

	protected List<Map<String, Object>> buildMessages(ChatRequest chatRequest) {
		List<Map<String, Object>> messages = new ArrayList<>();
		chatRequest.getConversations()
				.forEach(msg -> messages.add(Map.of("role", msg.getRole(), "content", msg.getContent())));
		messages.add(Map.of("role", "user", "content", chatRequest.getMessage()));
		return messages;
	}

	protected List<Map<String, Object>> buildTools() {
		return List.of(Map.of("name",
				"set_measures",
				"description",
				"Select the measures to display in the query result. Replaces any previously selected measures.",
				"input_schema",
				Map.of("type",
						"object",
						"properties",
						Map.of("measureNames",
								Map.of("type",
										"array",
										"items",
										Map.of("type", "string"),
										"description",
										"Exact measure names from the cube schema")),
						"required",
						List.of("measureNames"))),

				Map.of("name",
						"set_groupby",
						"description",
						"Set the groupBy dimensions (columns to aggregate by). Order matters — first column is the primary grouping.",
						"input_schema",
						Map.of("type",
								"object",
								"properties",
								Map.of("columns",
										Map.of("type",
												"array",
												"items",
												Map.of("type", "string"),
												"description",
												"Exact dimension column names from the cube schema")),
								"required",
								List.of("columns"))),

				Map.of("name",
						"clear_query",
						"description",
						"Reset all selections (measures and groupBy columns) to start a fresh query.",
						"input_schema",
						Map.of("type", "object", "properties", Map.of())));
	}

	protected Flux<String> callAnthropic(Map<String, Object> body) {
		Flux<ServerSentEvent<String>> rawStream = anthropicClient.post()
				.uri("/v1/messages")
				.contentType(MediaType.APPLICATION_JSON)
				.accept(MediaType.TEXT_EVENT_STREAM)
				.bodyValue(body)
				.retrieve()
				.bodyToFlux(new ParameterizedTypeReference<ServerSentEvent<String>>() {
				});

		return parseAnthropicStream(rawStream);
	}

	@SuppressWarnings("checkstyle:AvoidInlineConditionals")
	protected Flux<String> parseAnthropicStream(Flux<ServerSentEvent<String>> rawStream) {
		return Flux.create(sink -> {
			// Mutable state for the current content block
			final String[] blockType = { "none" };
			final String[] toolName = { "" };
			final StringBuilder toolInput = new StringBuilder();

			rawStream.subscribe(event -> {
				String data = event.data();
				if (data == null || "[DONE]".equals(data)) {
					return;
				}
				try {
					JsonNode node = objectMapper.readTree(data);
					String type = node.path("type").asText();

					switch (type) {
					case "content_block_start": {
						JsonNode block = node.path("content_block");
						blockType[0] = block.path("type").asText("none");
						if ("tool_use".equals(blockType[0])) {
							toolName[0] = block.path("name").asText();
							toolInput.setLength(0);
						}
						break;
					}
					case "content_block_delta": {
						JsonNode delta = node.path("delta");
						String deltaType = delta.path("type").asText();
						if ("text_delta".equals(deltaType)) {
							sink.next(toSseData(Map.of("type", "text", "content", delta.path("text").asText(""))));
						} else if ("input_json_delta".equals(deltaType)) {
							toolInput.append(delta.path("partial_json").asText(""));
						}
						break;
					}
					case "content_block_stop": {
						if ("tool_use".equals(blockType[0])) {
							String accumulated = toolInput.toString();
							JsonNode input = objectMapper.readTree(accumulated.isEmpty() ? "{}" : accumulated);
							sink.next(toSseData(Map.of("type", "tool_use", "name", toolName[0], "input", input)));
						}
						blockType[0] = "none";
						break;
					}
					case "message_stop": {
						sink.next(toSseData(Map.of("type", "done")));
						sink.complete();
						break;
					}
					default:
						break;
					}
				} catch (Exception e) {
					log.warn("Error parsing Anthropic SSE event: {}", data, e);
				}
			}, error -> {
				log.error("Anthropic stream error", error);
				try {
					sink.next(toSseData(Map.of("type", "error", "message", error.getMessage())));
				} catch (Exception ignored) {
					// best-effort: emit the error event before failing
				}
				sink.error(error);
			}, sink::complete);
		});
	}

	@SneakyThrows(JsonProcessingException.class)
	private String toSseData(Object event) {
		return "data: " + objectMapper.writeValueAsString(event) + "\n\n";
	}
}
