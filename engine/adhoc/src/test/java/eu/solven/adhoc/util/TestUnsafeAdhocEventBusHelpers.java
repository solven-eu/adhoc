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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.eventbus.AdhocEventsFromGuavaEventBusToSfl4j;
import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.eventbus.IAdhocEventBus;
import eu.solven.adhoc.eventbus.QueryLifecycleEvent;
import eu.solven.adhoc.eventbus.UnsafeAdhocEventBusHelpers;
import eu.solven.adhoc.table.InMemoryTable;

public class TestUnsafeAdhocEventBusHelpers {
	@Test
	public void testLogAndEventBus() {
		IAdhocEventBus eventBus = Mockito.mock(IAdhocEventBus.class);

		// TODO Ensure this is logged to SLF4J with `TestUnsafeAdhocEventBusHelpers` class
		UnsafeAdhocEventBusHelpers.logForkEventBus(eventBus,
				AdhocLogEvent.builder().message("someMessage").source(this).build());
	}

	@Test
	public void testLogAndEventBus_wrapped() {
		IAdhocEventBus eventBus = UnsafeAdhocEventBusHelpers.safeWrapper(Mockito.mock(IAdhocEventBus.class));

		// TODO Ensure this is logged to SLF4J with `TestUnsafeAdhocEventBusHelpers` class
		eventBus.post(AdhocLogEvent.builder().message("someMessage").source(this).build());
	}

	@Test
	public void testOnQueryLifecycleEvent() {
		IAdhocEventBus eventBus = UnsafeAdhocEventBusHelpers.safeWrapper(Mockito.mock(IAdhocEventBus.class));

		// TODO Current design lead to a difference between the logger and the logging class
		((Logger) LoggerFactory.getLogger(AdhocEventsFromGuavaEventBusToSfl4j.class)).setLevel(Level.DEBUG);

		// TODO Ensure this is logged to SLF4J with `TestUnsafeAdhocEventBusHelpers` class
		eventBus.post(QueryLifecycleEvent.builder()
				.query(QueryPod.forTable(InMemoryTable.builder().name("empty").build()))
				.build());
	}

}
