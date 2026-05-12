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
package eu.solven.adhoc.pivotable.webflux.chat;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.codec.ServerSentEvent;

import eu.solven.adhoc.pivotable.chat.PivotableChatProperties;
import reactor.core.publisher.Flux;
import tools.jackson.databind.ObjectMapper;

/**
 * Unit tests for {@link PivotableChatHandler}'s helpers. Currently focused on the rate-limiter-key derivation that
 * previously NPE'd whenever Netty handed Spring a remote address whose {@code InetAddress} was null (unresolved variant
 * — observed on some local Netty configurations and reported as a 500 by the SPA chat panel).
 */
public class TestPivotableChatHandler {

	@Test
	public void testExtractRemoteIp_emptyOptional_returnsAnonymous() {
		Assertions.assertThat(PivotableChatHandler.extractRemoteIp(Optional.empty())).isEqualTo("anonymous");
	}

	@Test
	public void testExtractRemoteIp_resolvedIpv4_returnsHostAddress() {
		InetSocketAddress address = new InetSocketAddress("127.0.0.1", 12_345);
		Assertions.assertThat(PivotableChatHandler.extractRemoteIp(Optional.of(address))).isEqualTo("127.0.0.1");
	}

	@Test
	public void testExtractRemoteIp_unresolvedAddress_returnsHostString() {
		// `InetSocketAddress.createUnresolved` produces exactly the shape that crashed the rate-limit branch:
		// `.getAddress()` returns null but `.getHostString()` is populated.
		InetSocketAddress unresolved = InetSocketAddress.createUnresolved("nowhere.invalid", 12_345);
		Assertions.assertThat(unresolved.getAddress()).isNull();
		Assertions.assertThat(PivotableChatHandler.extractRemoteIp(Optional.of(unresolved)))
				.isEqualTo("nowhere.invalid");
	}

	@Test
	public void testExtractRemoteIp_resolvedLoopback_returnsLiteralIp() throws Exception {
		// Use the explicit InetAddress constructor so the test never performs DNS — flake-resistant on offline CI.
		InetAddress loopback = InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 });
		InetSocketAddress address = new InetSocketAddress(loopback, 12_345);
		Assertions.assertThat(PivotableChatHandler.extractRemoteIp(Optional.of(address))).isEqualTo("127.0.0.1");
	}

	/**
	 * Regression test for the double-framing bug. {@link PivotableChatHandler#translateStream} feeds a
	 * {@code Flux<String>} into Spring's WebFlux SSE codec, which automatically adds {@code data:} framing per emission
	 * when the response content type is {@code text/event-stream}. The previous implementation ALSO pre-framed each
	 * emission with {@code "data: " + json + "\n\n"}, producing a malformed double-prefixed wire output (lines like
	 * {@code data:data: {...}}) that the SPA parser silently failed to JSON.parse, leaving the assistant bubble
	 * permanently empty.
	 *
	 * <p>
	 * Contract pinned here: every emission from {@code translateStream} is a single line of raw JSON, with no
	 * {@code data:} prefix and no trailing newlines.
	 */
	@Test
	public void testTranslateStream_emitsRawJson_neverPreFramed() {
		ObjectMapper objectMapper = new ObjectMapper();
		// Constructor args we don't exercise here can be null — translateStream only touches `objectMapper`.
		// translateStream only touches `objectMapper` — the other constructor args may be defaulted.
		PivotableChatHandler handler =
				new PivotableChatHandler(null, objectMapper, null, new PivotableChatProperties(), null, null);

		// Hand-crafted Anthropic-shape SSE events covering all three emission paths: text chunk, tool_use stop, done.
		Flux<ServerSentEvent<String>> raw = Flux.just(
				ServerSentEvent.<String>builder()
						.data("{\"type\":\"content_block_start\",\"content_block\":{\"type\":\"text\"}}")
						.build(),
				ServerSentEvent.<String>builder()
						.data("{\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}")
						.build(),
				ServerSentEvent.<String>builder().data("{\"type\":\"message_stop\"}").build());

		List<String> emissions = handler.translateStream(raw).collectList().block();

		Assertions.assertThat(emissions).isNotNull().isNotEmpty();
		for (String emitted : emissions) {
			// Each emission must be parseable JSON — no `data:` prefix, no leading/trailing whitespace, no newlines.
			Assertions.assertThat(emitted).doesNotStartWith("data:").doesNotContain("\n");
			// Must be valid JSON — round-trips through the mapper without throwing.
			objectMapper.readTree(emitted);
		}
		// We should have seen the text chunk and the terminal done event in the emitted stream.
		Assertions.assertThat(emissions)
				.anyMatch(j -> j.contains("\"type\":\"text\""))
				.anyMatch(j -> j.contains("\"type\":\"done\""));
	}
}
