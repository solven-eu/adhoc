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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.beta.schema.CubeSchemaMetadata;
import eu.solven.adhoc.beta.schema.EndpointSchemaMetadata;

/**
 * Builds the Anthropic Messages-API request body for the Pivotable chat: system prompt grounded in the cube schema,
 * conversation messages, and the tool catalog the agent may invoke to drive the query-builder UI.
 *
 * <p>
 * Framework-agnostic so that both the WebFlux router and the WebMVC controller can produce identical request bodies and
 * rely on the same {@link AnthropicSseTranslator} to simplify the streamed response.
 *
 * @author Benoit Lacelle
 */
public class ChatRequestPlanner {

	/** Default max_tokens passed to the Anthropic Messages API. */
	public static final int MAX_TOKENS = 1024;

	/**
	 * Maximum number of measures or columns dumped into the system prompt. Cap so a cube with hundreds of measures
	 * doesn't (a) blow the prompt budget or (b) leak the full schema to a casual user who only needs the common subset.
	 * Names beyond the cap are summarised as "… and N more".
	 */
	public static final int MAX_SCHEMA_ENTRIES_DUMPED = 50;

	/**
	 * Build the full Anthropic Messages API body for the given chat turn.
	 *
	 * @param request
	 *            the incoming {@link ChatRequest}
	 * @param metadata
	 *            schema metadata for the request's endpoint, used to ground the system prompt
	 * @param model
	 *            the Anthropic model identifier
	 * @return a JSON-serializable map matching the Anthropic Messages API contract
	 */
	public Map<String, Object> buildAnthropicBody(ChatRequest request, EndpointSchemaMetadata metadata, String model) {
		return buildAnthropicBody(request, metadata, model, false, ChatStyle.defaults());
	}

	public Map<String, Object> buildAnthropicBody(ChatRequest request,
			EndpointSchemaMetadata metadata,
			String model,
			boolean forceToolCall) {
		return buildAnthropicBody(request, metadata, model, forceToolCall, ChatStyle.defaults());
	}

	/**
	 * @param forceToolCall
	 *            when {@code true}, set Anthropic's {@code tool_choice: {"type":"any"}} so the model is required to end
	 *            its response with one of the declared tools. Mechanism (5) of the scoping demonstration — strongest
	 *            defence against off-topic prose, but disables clarifying questions.
	 * @param style
	 *            verbosity + ambiguity knobs woven into the system prompt; see {@link ChatStyle}
	 */
	public Map<String, Object> buildAnthropicBody(ChatRequest request,
			EndpointSchemaMetadata metadata,
			String model,
			boolean forceToolCall,
			ChatStyle style) {
		Map<String, Object> body = new LinkedHashMap<>();
		body.put("model", model);
		body.put("max_tokens", MAX_TOKENS);
		body.put("stream", true);
		body.put("system", buildSystemPrompt(request.getCube(), metadata, style));
		body.put("messages", buildMessages(request));
		body.put("tools", buildTools());
		if (forceToolCall) {
			body.put("tool_choice", ImmutableMap.of("type", "any"));
		}
		return body;
	}

	protected String buildSystemPrompt(String cube, EndpointSchemaMetadata metadata) {
		return buildSystemPrompt(cube, metadata, ChatStyle.defaults());
	}

	@SuppressWarnings({ "PMD.ConsecutiveAppendsShouldReuse",
			"PMD.ConsecutiveLiteralAppends",
			"PMD.AppendCharacterWithChar" })
	protected String buildSystemPrompt(String cube, EndpointSchemaMetadata metadata, ChatStyle style) {
		StringBuilder sb = new StringBuilder();
		sb.append("You are a helpful data analyst assistant embedded in a query builder UI.\n");
		sb.append("The user is building a query against OLAP cube '").append(cube).append("'.\n\n");

		CubeSchemaMetadata cubeSchema = metadata.getCubes().get(cube);
		if (cubeSchema != null) {
			appendCappedList(sb, "Available measures (use exact names):", cubeSchema.getMeasures().keySet());

			if (cubeSchema.getColumns() != null) {
				sb.append('\n');
				appendCappedList(sb,
						"Available dimension columns (use exact names):",
						cubeSchema.getColumns().getColumns().keySet());
			}
		}

		// Mechanism (1) of the scoping demonstration: an explicit SCOPE section instructing the model to refuse
		// off-topic requests with a fixed sentence. Defence-in-depth — the model can still be jailbroken with
		// adversarial
		// prompts, but a casual user asking "write me a poem" gets a brief refusal instead of free GPU time.
		sb.append("""

				SCOPE — You may ONLY help the user build a query against this specific cube.
				REFUSE any request that is not about:
				  - selecting measures or groupBy columns on this cube
				  - explaining what a measure / column does (based on its name)
				  - interpreting query results the user just ran

				For anything else (coding help, general chit-chat, other cubes, world news, math
				puzzles, image generation, jokes, role-play) reply with ONLY this single sentence
				and do not call any tool:
				  "I can only help build queries against the '%s' cube."

				When the request IS in scope, call the appropriate tools:
				- set_measures: select which measures to display (e.g. "show revenue" → select Revenue.SUM)
				- set_groupby: set groupBy dimensions (e.g. "by country" → add Country column)
				- clear_query: reset all selections when the user wants to start over

				Rules:
				- Always use EXACT names from the schema above.
				- You may call multiple tools in one turn (e.g. set_measures AND set_groupby together).
				- Plain-text reply: AT MOST %d short sentence%s. No preamble, no recap, no offers to do more.
				%s
				""".formatted(cube,
				style.getMaxSentences(),
				style.getMaxSentences() == 1 ? "" : "s",
				ambiguityRule(style.getAmbiguity())));

		return sb.toString();
	}

	/** Renders the {@link ChatStyle.Ambiguity} knob into a one-line prompt rule. */
	private String ambiguityRule(ChatStyle.Ambiguity ambiguity) {
		if (ambiguity == ChatStyle.Ambiguity.CLARIFY) {
			return "- If the request is ambiguous, ask a one-sentence clarifying question before calling any tool.";
		}
		// BEST_GUESS — the new default. Encourages the model to match measure/column names by similarity rather than
		// stalling on a clarification round-trip.
		return "- Never ask for clarification. Pick the measure / column whose name most closely matches the user's"
				+ " wording (substring, prefix, or fuzzy match) and call the tool. The user will correct you with a"
				+ " follow-up if your guess is wrong — that round-trip is cheaper than a clarifying question.";
	}

	/**
	 * Append a bullet-list of names to the prompt, capped at {@link #MAX_SCHEMA_ENTRIES_DUMPED} entries. Mechanism (2)
	 * of the scoping demonstration — limits the data surface area the model sees per turn. In a future enhancement,
	 * this would also filter by the current user's authorisations (not plumbed yet).
	 */
	private void appendCappedList(StringBuilder sb, String header, Iterable<String> names) {
		sb.append(header).append('\n');
		List<String> sorted = java.util.stream.StreamSupport.stream(names.spliterator(), false).sorted().toList();
		int capped = Math.min(sorted.size(), MAX_SCHEMA_ENTRIES_DUMPED);
		for (int i = 0; i < capped; i++) {
			sb.append("  - ").append(sorted.get(i)).append('\n');
		}
		if (sorted.size() > MAX_SCHEMA_ENTRIES_DUMPED) {
			sb.append("  … and ").append(sorted.size() - MAX_SCHEMA_ENTRIES_DUMPED).append(" more (ask if needed)\n");
		}
	}

	protected List<Map<String, Object>> buildMessages(ChatRequest request) {
		List<Map<String, Object>> messages = new ArrayList<>();
		request.getConversations()
				.forEach(msg -> messages.add(ImmutableMap.of("role", msg.getRole(), "content", msg.getContent())));
		messages.add(ImmutableMap.of("role", "user", "content", request.getMessage()));
		return messages;
	}

	@SuppressWarnings("PMD.AvoidDuplicateLiterals")
	protected List<Map<String, Object>> buildTools() {
		return ImmutableList.of(ImmutableMap.<String, Object>builder()
				.put("name", "set_measures")
				.put("description",
						"Select the measures to display in the query result. Replaces any previously selected measures.")
				.put("input_schema",
						ImmutableMap.<String, Object>builder()
								.put("type", "object")
								.put("additionalProperties", false)
								.put("properties",
										ImmutableMap.of("measureNames",
												ImmutableMap.<String, Object>builder()
														.put("type", "array")
														.put("items", ImmutableMap.of("type", "string"))
														.put("description", "Exact measure names from the cube schema")
														.build()))
								.put("required", ImmutableList.of("measureNames"))
								.build())
				.build(),

				ImmutableMap.<String, Object>builder()
						.put("name", "set_groupby")
						.put("description",
								"Set the groupBy dimensions (columns to aggregate by). Order matters — first column is the primary grouping.")
						.put("input_schema",
								ImmutableMap.<String, Object>builder()
										.put("type", "object")
										.put("properties",
												ImmutableMap.of("columns",
														ImmutableMap.<String, Object>builder()
																.put("type", "array")
																.put("items", ImmutableMap.of("type", "string"))
																.put("description",
																		"Exact dimension column names from the cube schema")
																.build()))
										.put("required", ImmutableList.of("columns"))
										.build())
						.build(),

				ImmutableMap.<String, Object>builder()
						.put("name", "clear_query")
						.put("description",
								"Reset all selections (measures and groupBy columns) to start a fresh query.")
						.put("input_schema",
								ImmutableMap.<String, Object>builder()
										.put("type", "object")
										.put("properties", ImmutableMap.of())
										.build())
						.build());
	}
}
