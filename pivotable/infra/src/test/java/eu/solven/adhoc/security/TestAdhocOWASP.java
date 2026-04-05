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
package eu.solven.adhoc.security;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAdhocOWASP {

	@Test
	public void sanitizeLog_null() {
		Assertions.assertThat(AdhocOWASP.sanitizeLog(null)).isNull();
	}

	@Test
	public void sanitizeLog_clean() {
		Assertions.assertThat(AdhocOWASP.sanitizeLog("abc-123")).isEqualTo("abc-123");
	}

	@Test
	public void sanitizeLog_lineFeed() {
		Assertions.assertThat(AdhocOWASP.sanitizeLog("injected\nfake log line")).isEqualTo("injected_fake log line");
	}

	@Test
	public void sanitizeLog_carriageReturn() {
		Assertions.assertThat(AdhocOWASP.sanitizeLog("injected\rfake log line")).isEqualTo("injected_fake log line");
	}

	@Test
	public void sanitizeLog_crlf() {
		Assertions.assertThat(AdhocOWASP.sanitizeLog("injected\r\nfake log line")).isEqualTo("injected__fake log line");
	}

	@Test
	public void sanitizeLog_nel() {
		// U+0085 NEXT LINE
		Assertions.assertThat(AdhocOWASP.sanitizeLog("injected\u0085fake log line"))
				.isEqualTo("injected_fake log line");
	}

	@Test
	public void sanitizeLog_lineSeparator() {
		// U+2028 LINE SEPARATOR
		Assertions.assertThat(AdhocOWASP.sanitizeLog("injected\u2028fake log line"))
				.isEqualTo("injected_fake log line");
	}

	@Test
	public void sanitizeLog_paragraphSeparator() {
		// U+2029 PARAGRAPH SEPARATOR
		Assertions.assertThat(AdhocOWASP.sanitizeLog("injected\u2029fake log line"))
				.isEqualTo("injected_fake log line");
	}

	@Test
	public void sanitizeLog_empty() {
		Assertions.assertThat(AdhocOWASP.sanitizeLog("")).isEqualTo("");
	}
}
