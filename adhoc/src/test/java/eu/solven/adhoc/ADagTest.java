package eu.solven.adhoc;

import org.greenrobot.eventbus.EventBus;
import org.junit.jupiter.api.BeforeEach;

import eu.solven.adhoc.dag.AdhocMeasureBag;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.database.InMemoryDatabase;
import eu.solven.adhoc.eventbus.AdhocEventsToSfl4j;

/**
 * Helps testing anything related with a {@link AdhocMeasureBag} or a {@link AdhocQueryEngine}
 * 
 * @author Benoit Lacelle
 *
 */
public abstract class ADagTest {
	final EventBus eventBus = new EventBus();
	final AdhocEventsToSfl4j toSlf4j = new AdhocEventsToSfl4j();
	final public AdhocMeasureBag amb = AdhocMeasureBag.builder().build();
	final public AdhocQueryEngine aqe = AdhocQueryEngine.builder().eventBus(eventBus).measureBag(amb).build();

	final public InMemoryDatabase rows = InMemoryDatabase.builder().build();

	@BeforeEach
	public void wireEvents() {
		eventBus.register(toSlf4j);
	}

	// `@BeforeEach` has to be duplicated on each implementation
	// @BeforeEach
	public abstract void feedDb();

}
