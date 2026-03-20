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
package eu.solven.adhoc.eventbus;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

public class TestAdhocLogEvent {

	@Test
	public void testBuild_minimal() {
		AdhocLogEvent event = AdhocLogEvent.builder().message("hello").source(this).build();

		Assertions.assertThat(event.getMessage()).isEqualTo("hello");
		Assertions.assertThat(event.getSource()).isSameAs(this);
		// defaults
		Assertions.assertThat(event.isExplain()).isFalse();
		Assertions.assertThat(event.isDebug()).isFalse();
		Assertions.assertThat(event.isPerformance()).isFalse();
		Assertions.assertThat(event.getLevel()).isEqualTo(Level.INFO);
		Assertions.assertThat(event.getTags()).isEmpty();
	}

	@Test
	public void testMessageT_slf4jPlaceholder() {
		AdhocLogEvent event = AdhocLogEvent.builder().messageT("value={} and other={}", 42, "x").source(this).build();

		Assertions.assertThat(event.getMessage()).isEqualTo("value=42 and other=x");
	}

	@Test
	public void testMessageT_percentSPlaceholder() {
		AdhocLogEvent event = AdhocLogEvent.builder().messageT("value=%s", "hello").source(this).build();

		Assertions.assertThat(event.getMessage()).isEqualTo("value=hello");
	}

	@Test
	public void testBuild_withExplainAndDebug() {
		AdhocLogEvent event =
				AdhocLogEvent.builder().message("m").source(this).explain(true).debug(true).level(Level.DEBUG).build();

		Assertions.assertThat(event.isExplain()).isTrue();
		Assertions.assertThat(event.isDebug()).isTrue();
		Assertions.assertThat(event.getLevel()).isEqualTo(Level.DEBUG);
	}

	@Test
	public void testWithFqdn() {
		AdhocLogEvent event = AdhocLogEvent.builder().message("m").source(this).build();
		AdhocLogEvent withFqdn = event.withFqdn("some.fqdn");

		Assertions.assertThat(withFqdn.getFqdn()).isEqualTo("some.fqdn");
		// original unchanged
		Assertions.assertThat(event.getFqdn()).isNull();
	}
}
