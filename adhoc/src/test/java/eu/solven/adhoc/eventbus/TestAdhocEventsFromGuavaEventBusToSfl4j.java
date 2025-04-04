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
package eu.solven.adhoc.eventbus;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAdhocEventsFromGuavaEventBusToSfl4j {
	@Test
	public void testExplainPerfAreSplitByEOL() {
		AdhocEventsFromGuavaEventBusToSfl4j toSlf4j = new AdhocEventsFromGuavaEventBusToSfl4j();

		AdhocLogEvent event = AdhocLogEvent.builder()
				.explain(true)
				.message("someMessage\r\nsomeSubMessage")
				.source("someSource")
				.build();

		List<String> receivedRows = new ArrayList<>();
		BiConsumer<String, Object[]> logPrinter = (msg, values) -> {
			// We receive SLF4J templates
			Assertions.assertThat(msg).contains("{}").doesNotContain("%s");

			String wholeMsg = msg.replaceAll("\\{\\}", "%s").formatted(values);
			receivedRows.add(wholeMsg);
		};
		toSlf4j.printLogEvent(event, logPrinter);

		Assertions.assertThat(receivedRows).hasSize(2).satisfiesOnlyOnce(wholeMsg -> {
			Assertions.assertThat(wholeMsg).contains("someMessage").doesNotContain("someSubMessage");
		}).satisfiesOnlyOnce(wholeMsg -> {
			Assertions.assertThat(wholeMsg).doesNotContain("someMessage").contains("someSubMessage");
		});
	}
}
