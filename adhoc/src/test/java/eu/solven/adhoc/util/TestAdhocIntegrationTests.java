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
package eu.solven.adhoc.util;

import java.util.Optional;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.core.env.StandardEnvironment;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestAdhocIntegrationTests {
	// To be referred by `@EnabledIf` for integration tests
	public static final String ENABLED_IF = "eu.solven.adhoc.util.TestAdhocIntegrationTests" + "#runIntegrationTests";

	public static boolean runIntegrationTests() {
		Optional<StackTraceElement> ideEntry = Stream.of(Thread.currentThread().getStackTrace())
				.filter(e -> e.getClassName().startsWith("org.eclipse.jdt."))
				.findAny();
		if (ideEntry.isPresent()) {
			log.info("Running {} due to {}", "IntegrationTests", ideEntry.get());
			return true;
		}

		String valueAsString = new StandardEnvironment().getProperty("adhoc.runIntegrationTests", String.class);

		if (valueAsString == null) {
			return false;
		} else if (valueAsString.isEmpty()) {
			return true;
		} else {
			return Boolean.parseBoolean(valueAsString);
		}
	}

	/**
	 * Check the integration tests are disabled by default
	 */
	@DisabledIf(TestAdhocIntegrationTests.ENABLED_IF)
	@Test
	public void testSkipIntegrationsTests() {
		Assertions.assertThat(runIntegrationTests()).isFalse();
	}

	@EnabledIf(TestAdhocIntegrationTests.ENABLED_IF)
	@Test
	public void testRunIntegrationsTests() {
		Assertions.assertThat(runIntegrationTests()).isTrue();
	}

}
