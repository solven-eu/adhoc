package eu.solven.adhoc.util;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.eventbus.AdhocEventsFromGuavaEventBusToSfl4j;
import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.eventbus.QueryLifecycleEvent;
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
