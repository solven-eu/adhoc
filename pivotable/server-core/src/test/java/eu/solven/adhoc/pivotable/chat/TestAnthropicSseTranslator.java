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
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

public class TestAnthropicSseTranslator {

	// Plain ObjectMapper (no pretty-printing) — mirrors the Spring Boot autoconfigured default used in production
	// rather than the AdhocJackson factory which enables INDENT_OUTPUT.
	final ObjectMapper objectMapper = new ObjectMapper();

	private JsonNode parse(String json) {
		return objectMapper.readTree(json);
	}

	@Test
	public void testTextDelta_emitsTextEvent() {
		AnthropicSseTranslator translator = new AnthropicSseTranslator(objectMapper);
		List<String> emitted = new ArrayList<>();

		translator.onAnthropicEvent("{\"type\":\"content_block_start\",\"content_block\":{\"type\":\"text\"}}",
				emitted::add);
		translator.onAnthropicEvent(
				"{\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}",
				emitted::add);

		Assertions.assertThat(emitted).hasSize(1);
		JsonNode ev = parse(emitted.get(0));
		Assertions.assertThat(ev.get("type").asString()).isEqualTo("text");
		Assertions.assertThat(ev.get("content").asString()).isEqualTo("Hello");
	}

	@Test
	public void testToolUse_accumulatedThenEmittedOnStop() {
		AnthropicSseTranslator translator = new AnthropicSseTranslator(objectMapper);
		List<String> emitted = new ArrayList<>();

		translator.onAnthropicEvent(
				"{\"type\":\"content_block_start\",\"content_block\":{\"type\":\"tool_use\",\"name\":\"set_measures\"}}",
				emitted::add);
		translator.onAnthropicEvent(
				"{\"type\":\"content_block_delta\",\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"measureNames\\\":[\"}}",
				emitted::add);
		translator.onAnthropicEvent(
				"{\"type\":\"content_block_delta\",\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\\\"delta\\\"]}\"}}",
				emitted::add);
		// Nothing emitted yet — tool_use is only flushed at content_block_stop.
		Assertions.assertThat(emitted).isEmpty();

		translator.onAnthropicEvent("{\"type\":\"content_block_stop\"}", emitted::add);

		Assertions.assertThat(emitted).hasSize(1);
		JsonNode ev = parse(emitted.get(0));
		Assertions.assertThat(ev.get("type").asString()).isEqualTo("tool_use");
		Assertions.assertThat(ev.get("name").asString()).isEqualTo("set_measures");
		Assertions.assertThat(ev.get("input").get("measureNames").get(0).asString()).isEqualTo("delta");
	}

	@Test
	public void testMessageStop_emitsDone() {
		AnthropicSseTranslator translator = new AnthropicSseTranslator(objectMapper);
		List<String> emitted = new ArrayList<>();

		translator.onAnthropicEvent("{\"type\":\"message_stop\"}", emitted::add);

		Assertions.assertThat(emitted).hasSize(1);
		Assertions.assertThat(parse(emitted.get(0)).get("type").asString()).isEqualTo("done");
	}

	@Test
	public void testNullAndDoneSentinel_ignored() {
		AnthropicSseTranslator translator = new AnthropicSseTranslator(objectMapper);
		List<String> emitted = new ArrayList<>();

		translator.onAnthropicEvent(null, emitted::add);
		translator.onAnthropicEvent("[DONE]", emitted::add);

		Assertions.assertThat(emitted).isEmpty();
	}

	@Test
	public void testMalformedJson_doesNotThrow() {
		AnthropicSseTranslator translator = new AnthropicSseTranslator(objectMapper);
		List<String> emitted = new ArrayList<>();

		translator.onAnthropicEvent("not-json", emitted::add);

		Assertions.assertThat(emitted).isEmpty();
	}

	@Test
	public void testErrorEvent_includesMessage() {
		AnthropicSseTranslator translator = new AnthropicSseTranslator(objectMapper);

		JsonNode ev = parse(translator.errorEvent("boom"));
		Assertions.assertThat(ev.get("type").asString()).isEqualTo("error");
		Assertions.assertThat(ev.get("message").asString()).isEqualTo("boom");
	}

	@Test
	public void testErrorEvent_nullMessage_becomesEmpty() {
		AnthropicSseTranslator translator = new AnthropicSseTranslator(objectMapper);

		JsonNode ev = parse(translator.errorEvent(null));
		Assertions.assertThat(ev.get("type").asString()).isEqualTo("error");
		Assertions.assertThat(ev.get("message").asString()).isEmpty();
	}
}
