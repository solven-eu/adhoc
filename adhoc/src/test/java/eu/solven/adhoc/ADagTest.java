package eu.solven.adhoc;

import org.greenrobot.eventbus.EventBus;
import org.junit.jupiter.api.BeforeEach;

import eu.solven.adhoc.dag.AdhocMeasuresSet;
import eu.solven.adhoc.database.InMemoryDatabase;
import eu.solven.adhoc.eventbus.AdhocEventsToSfl4j;

public abstract class ADagTest {
	final EventBus eventBus = new EventBus();
	final AdhocEventsToSfl4j toSlf4j = new AdhocEventsToSfl4j();
	final public AdhocMeasuresSet dag = AdhocMeasuresSet.builder().eventBus(eventBus).build();

	protected final InMemoryDatabase rows = InMemoryDatabase.builder().build();

	@BeforeEach
	public void wireEvents() {
		eventBus.register(toSlf4j);
	}

	// `@BeforeEach` has to be duplicated on each implementation
	// @BeforeEach
	public abstract void feedDb();

}
