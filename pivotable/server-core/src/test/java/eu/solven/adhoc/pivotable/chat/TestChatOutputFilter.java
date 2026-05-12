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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestChatOutputFilter {

	final ChatOutputFilter filter = new ChatOutputFilter();

	@Test
	public void testPlainText_passesThrough() {
		Assertions.assertThat(filter.filter("Hello, here are your measures."))
				.isEqualTo("Hello, here are your measures.");
	}

	@Test
	public void testAwsAccessKeyId_redacted() {
		String redacted = filter.filter("Try AKIAIOSFODNN7EXAMPLE in your config.");
		Assertions.assertThat(redacted).doesNotContain("AKIA").contains(ChatOutputFilter.REDACTED);
	}

	@Test
	public void testAnthropicKey_redacted() {
		String redacted = filter.filter("Use sk-ant-api03-abcdefghijklmnopqrstuvwxyz123456 to authenticate.");
		Assertions.assertThat(redacted).doesNotContain("sk-ant-api03-abcdefghijkl").contains(ChatOutputFilter.REDACTED);
	}

	@Test
	public void testOat01Token_redacted() {
		String redacted = filter.filter("CLI token: sk-ant-oat01-ABCDEFGHIJ-1234567890_xyz");
		Assertions.assertThat(redacted).doesNotContain("sk-ant-oat01-ABCDEF").contains(ChatOutputFilter.REDACTED);
	}

	@Test
	public void testCreditCardLike_redacted() {
		Assertions.assertThat(filter.filter("Card: 4111-1111-1111-1111")).contains(ChatOutputFilter.REDACTED);
		Assertions.assertThat(filter.filter("Card: 4111 1111 1111 1111")).contains(ChatOutputFilter.REDACTED);
		Assertions.assertThat(filter.filter("Card: 4111111111111111")).contains(ChatOutputFilter.REDACTED);
	}

	@Test
	public void testShellHazard_redacted() {
		Assertions.assertThat(filter.filter("Run `rm -rf /tmp/foo` to clean up.")).contains(ChatOutputFilter.REDACTED);
	}

	@Test
	public void testMultiplePatterns_inOneChunk() {
		String input = "Set AWS_KEY=AKIAIOSFODNN7EXAMPLE and ANTHROPIC=sk-ant-api03-abc123def456ghi789jkl012";
		String redacted = filter.filter(input);
		Assertions.assertThat(redacted).doesNotContain("AKIA").doesNotContain("sk-ant-api03-abc");
	}
}
