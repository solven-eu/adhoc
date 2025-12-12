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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

public class TestAdhocEventsFromGuavaEventBusToSfl4j {
	AdhocEventsFromGuavaEventBusToSfl4j toSlf4j = new AdhocEventsFromGuavaEventBusToSfl4j();
	List<String> receivedRows = new ArrayList<>();

	BiConsumer<String, Object[]> logPrinter = (msg, values) -> {
		// We receive SLF4J templates
		Assertions.assertThat(msg).contains("{}").doesNotContain("%s");

		String wholeMsg = msg.replaceAll("\\{\\}", "%s").formatted(values);
		receivedRows.add(wholeMsg);
	};

	// https://stackoverflow.com/questions/29076981/how-to-intercept-slf4j-with-logback-logging-via-a-junit-test
	private ListAppender<ILoggingEvent> logWatcher;

	@BeforeEach
	void setup() {
		logWatcher = new ListAppender<>();
		logWatcher.start();
		((Logger) LoggerFactory.getLogger(AdhocEventsFromGuavaEventBusToSfl4j.class)).addAppender(logWatcher);
	}

	@Test
	public void testExplainPerfAreSplitByEOL() {
		AdhocLogEvent event = AdhocLogEvent.builder()
				.explain(true)
				.message("someMessage\r\nsomeSubMessage")
				.source("someSource")
				.build();

		toSlf4j.printLogEvent(event, logPrinter);

		Assertions.assertThat(receivedRows).hasSize(2).satisfiesOnlyOnce(wholeMsg -> {
			Assertions.assertThat(wholeMsg).contains("someMessage").doesNotContain("someSubMessage");
		}).satisfiesOnlyOnce(wholeMsg -> {
			Assertions.assertThat(wholeMsg).doesNotContain("someMessage").contains("someSubMessage");
		});

		Assertions.assertThat(logWatcher.list).isEmpty();
	}

	@Test
	public void testCustomEvent() {
		// Some custom event
		IAdhocEvent customEvent = new IAdhocEvent() {

			@Override
			public String getFqdn() {
				throw new UnsupportedOperationException("Irrelevant");
			}

			@Override
			public IAdhocEvent withFqdn(String fqdn) {
				throw new UnsupportedOperationException("Irrelevant");
			}
		};

		toSlf4j.onAdhocEvent(customEvent);

		Assertions.assertThat(receivedRows).isEmpty();

		Assertions.assertThat(logWatcher.list).hasSize(1).anySatisfy(le -> {
			Assertions.assertThat(le.getLevel()).isEqualTo(Level.WARN);
		});
	}
}
