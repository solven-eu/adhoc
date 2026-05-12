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

import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;

import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Translates the verbose Anthropic Messages-streaming protocol into the simplified SSE event vocabulary the Pivotable
 * chatbot frontend understands:
 * <ul>
 * <li>{@code {"type":"text","content":"..."}} — incremental text chunk to render</li>
 * <li>{@code {"type":"tool_use","name":"...","input":{...}}} — fully-assembled tool call</li>
 * <li>{@code {"type":"done"}} — terminal marker</li>
 * <li>{@code {"type":"error","message":"..."}} — error marker</li>
 * </ul>
 *
 * <p>
 * Instances are stateful: a single translator must be used for a single stream because it accumulates the in-flight
 * tool-input JSON across {@code content_block_delta} events. Not thread-safe.
 *
 * @author Benoit Lacelle
 */
@Slf4j
@SuppressWarnings("PMD.AvoidStringBufferField")
// AvoidStringBufferField targets long-lived owners that could leak the buffer; translator instances are per-stream
// (created fresh in the chat handler / controller and discarded on stream close), so the lifetime is bounded.
public class AnthropicSseTranslator {

	/** JSON field name reused across every emitted event payload. Factored out to silence AvoidDuplicateLiterals. */
	private static final String F_TYPE = "type";

	final ObjectMapper objectMapper;

	final ChatOutputFilter outputFilter;

	// Type of the content block currently being assembled ("text", "tool_use", or "none").
	private String blockType = "none";

	// Name of the tool call currently being assembled (empty when not in a tool_use block).
	private String toolName = "";

	// Accumulator for the streamed input_json_delta fragments forming the current tool_use input.
	private final StringBuilder toolInput = new StringBuilder();

	public AnthropicSseTranslator(ObjectMapper objectMapper) {
		this(objectMapper, new ChatOutputFilter());
	}

	public AnthropicSseTranslator(ObjectMapper objectMapper, ChatOutputFilter outputFilter) {
		this.objectMapper = objectMapper;
		this.outputFilter = outputFilter;
	}

	/**
	 * Process one Anthropic SSE data payload (the JSON string after {@code data: }) and emit zero or more simplified
	 * SSE data payloads to the sink.
	 *
	 * @param data
	 *            the raw Anthropic event JSON
	 * @param sink
	 *            consumer that receives simplified SSE data payloads (each is a JSON string; the caller is responsible
	 *            for wrapping with {@code data: } / {@code \n\n})
	 */
	public void onAnthropicEvent(String data, Consumer<String> sink) {
		if (data == null || "[DONE]".equals(data)) {
			return;
		}
		try {
			JsonNode node = objectMapper.readTree(data);
			String type = node.path(F_TYPE).asString();

			switch (type) {
			case "content_block_start": {
				JsonNode block = node.path("content_block");
				blockType = block.path(F_TYPE).asString("none");
				if ("tool_use".equals(blockType)) {
					toolName = block.path("name").asString();
					toolInput.setLength(0);
				}
				break;
			}
			case "content_block_delta": {
				JsonNode delta = node.path("delta");
				String deltaType = delta.path(F_TYPE).asString();
				if ("text_delta".equals(deltaType)) {
					// Mechanism (4): redact secrets / shell hazards before forwarding the text chunk to the SPA.
					String filtered = outputFilter.filter(delta.path("text").asString(""));
					sink.accept(toJson(ImmutableMap.of(F_TYPE, "text", "content", filtered)));
				} else if ("input_json_delta".equals(deltaType)) {
					toolInput.append(delta.path("partial_json").asString(""));
				}
				break;
			}
			case "content_block_stop": {
				if ("tool_use".equals(blockType)) {
					String inputJson = toolInput.toString();
					if (inputJson.isEmpty()) {
						inputJson = "{}";
					}
					JsonNode input = objectMapper.readTree(inputJson);
					sink.accept(toJson(ImmutableMap.of(F_TYPE, "tool_use", "name", toolName, "input", input)));
				}
				blockType = "none";
				break;
			}
			case "message_stop": {
				sink.accept(toJson(ImmutableMap.of(F_TYPE, "done")));
				break;
			}
			default:
				break;
			}
		} catch (RuntimeException e) {
			log.warn("Error parsing Anthropic SSE event: {}", data, e);
		}
	}

	/**
	 * Format an error event as a simplified SSE data payload. Callers should forward this when the upstream stream
	 * terminates abnormally.
	 *
	 * @param message
	 *            the error message to surface to the frontend
	 * @return the JSON payload (without {@code data: }/{@code \n\n} framing)
	 */
	public String errorEvent(String message) {
		String safeMessage = message;
		if (safeMessage == null) {
			safeMessage = "";
		}
		return toJson(ImmutableMap.of(F_TYPE, "error", "message", safeMessage));
	}

	private String toJson(Object event) {
		return objectMapper.writeValueAsString(event);
	}
}
