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

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.beta.schema.CubeSchemaMetadata;
import eu.solven.adhoc.beta.schema.EndpointSchemaMetadata;

public class TestChatRequestPlanner {

	final ChatRequestPlanner planner = new ChatRequestPlanner();

	private EndpointSchemaMetadata emptyMetadata() {
		return EndpointSchemaMetadata.builder().build();
	}

	private ChatRequest sampleRequest() {
		return ChatRequest.builder()
				.endpointId(UUID.fromString("00000000-0000-0000-0000-000000000000"))
				.cube("simple")
				.message("show delta")
				.build();
	}

	@Test
	public void testBuildAnthropicBody_topLevelKeys() {
		Map<String, Object> body = planner.buildAnthropicBody(sampleRequest(), emptyMetadata(), "claude-haiku-4-5");

		Assertions.assertThat(body)
				.containsEntry("model", "claude-haiku-4-5")
				.containsEntry("max_tokens", ChatRequestPlanner.MAX_TOKENS)
				.containsEntry("stream", true)
				.containsKey("system")
				.containsKey("messages")
				.containsKey("tools");
	}

	@Test
	public void testBuildAnthropicBody_messagesIncludesCurrentTurn() {
		ChatRequest request = ChatRequest.builder()
				.endpointId(UUID.fromString("00000000-0000-0000-0000-000000000000"))
				.cube("simple")
				.message("show delta")
				.conversation(ChatMessage.builder().role("user").content("hi").build())
				.conversation(ChatMessage.builder().role("assistant").content("hello").build())
				.build();

		Map<String, Object> body = planner.buildAnthropicBody(request, emptyMetadata(), "claude-haiku-4-5");

		@SuppressWarnings("unchecked")
		List<Map<String, Object>> messages = (List<Map<String, Object>>) body.get("messages");
		Assertions.assertThat(messages).hasSize(3);
		Assertions.assertThat(messages.get(0)).containsEntry("role", "user").containsEntry("content", "hi");
		Assertions.assertThat(messages.get(1)).containsEntry("role", "assistant").containsEntry("content", "hello");
		Assertions.assertThat(messages.get(2)).containsEntry("role", "user").containsEntry("content", "show delta");
	}

	@Test
	public void testBuildAnthropicBody_toolsCatalog() {
		Map<String, Object> body = planner.buildAnthropicBody(sampleRequest(), emptyMetadata(), "claude-haiku-4-5");

		@SuppressWarnings("unchecked")
		List<Map<String, Object>> tools = (List<Map<String, Object>>) body.get("tools");
		Assertions.assertThat(tools).hasSize(3);
		Assertions.assertThat(tools)
				.extracting(t -> t.get("name"))
				.containsExactly("set_measures", "set_groupby", "clear_query");
	}

	@Test
	public void testBuildSystemPrompt_unknownCube_omitsSchemaSection() {
		Map<String, Object> body = planner.buildAnthropicBody(sampleRequest(), emptyMetadata(), "claude-haiku-4-5");

		String prompt = (String) body.get("system");
		Assertions.assertThat(prompt).contains("cube 'simple'").doesNotContain("Available measures");
	}

	@Test
	public void testBuildSystemPrompt_knownCube_listsMeasures() {
		CubeSchemaMetadata cubeSchema = CubeSchemaMetadata.builder()
				.measures(ImmutableMap.of("delta",
						eu.solven.adhoc.model.measure.Aggregator.sum("delta"),
						"gamma",
						eu.solven.adhoc.model.measure.Aggregator.sum("gamma")))
				.build();
		EndpointSchemaMetadata metadata = EndpointSchemaMetadata.builder().cube("simple", cubeSchema).build();

		Map<String, Object> body = planner.buildAnthropicBody(sampleRequest(), metadata, "claude-haiku-4-5");

		String prompt = (String) body.get("system");
		Assertions.assertThat(prompt).contains("Available measures").contains("delta").contains("gamma");
	}

	// Validates buildMessages handles the "no history" first-turn case where ChatRequest.conversations is empty.
	@Test
	public void testBuildAnthropicBody_emptyConversations_onlyCurrentMessage() {
		ChatRequest request = ChatRequest.builder()
				.endpointId(UUID.fromString("00000000-0000-0000-0000-000000000000"))
				.cube("simple")
				.message("first turn")
				.conversations(ImmutableList.of())
				.build();

		Map<String, Object> body = planner.buildAnthropicBody(request, emptyMetadata(), "claude-haiku-4-5");

		@SuppressWarnings("unchecked")
		List<Map<String, Object>> messages = (List<Map<String, Object>>) body.get("messages");
		Assertions.assertThat(messages).hasSize(1);
		Assertions.assertThat(messages.get(0)).containsEntry("role", "user").containsEntry("content", "first turn");
	}
}
