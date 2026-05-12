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

import java.util.Map;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.beta.schema.EndpointSchemaMetadata;

public class TestChatStyle {

	final ChatRequestPlanner planner = new ChatRequestPlanner();

	private ChatRequest sampleRequest() {
		return ChatRequest.builder()
				.endpointId(UUID.fromString("00000000-0000-0000-0000-000000000000"))
				.cube("simple")
				.message("show delta")
				.build();
	}

	@Test
	public void testDefaults_areSyntheticBestGuess() {
		ChatStyle style = ChatStyle.defaults();

		Assertions.assertThat(style.getMaxSentences()).isEqualTo(2);
		Assertions.assertThat(style.getAmbiguity()).isEqualTo(ChatStyle.Ambiguity.BEST_GUESS);
	}

	@Test
	public void testDefaultStyle_promptCapsAtTwoSentences_andForbidsClarifications() {
		Map<String, Object> body =
				planner.buildAnthropicBody(sampleRequest(), EndpointSchemaMetadata.builder().build(), "m");
		String prompt = (String) body.get("system");

		Assertions.assertThat(prompt).contains("AT MOST 2 short sentences").contains("Never ask for clarification");
	}

	@Test
	public void testClarifyStyle_promptKeepsClarificationOption() {
		ChatStyle clarify = ChatStyle.builder().ambiguity(ChatStyle.Ambiguity.CLARIFY).build();

		Map<String, Object> body = planner
				.buildAnthropicBody(sampleRequest(), EndpointSchemaMetadata.builder().build(), "m", false, clarify);
		String prompt = (String) body.get("system");

		Assertions.assertThat(prompt).contains("ask a one-sentence clarifying question").doesNotContain("Never ask");
	}

	@Test
	public void testMaxSentences_pluralAndSingularForms() {
		ChatStyle one = ChatStyle.builder().maxSentences(1).build();
		ChatStyle five = ChatStyle.builder().maxSentences(5).build();

		String promptOne = (String) planner
				.buildAnthropicBody(sampleRequest(), EndpointSchemaMetadata.builder().build(), "m", false, one)
				.get("system");
		String promptFive = (String) planner
				.buildAnthropicBody(sampleRequest(), EndpointSchemaMetadata.builder().build(), "m", false, five)
				.get("system");

		// Singular and plural rendering — checks the formatter branch in buildSystemPrompt.
		Assertions.assertThat(promptOne).contains("AT MOST 1 short sentence.");
		Assertions.assertThat(promptFive).contains("AT MOST 5 short sentences.");
	}
}
