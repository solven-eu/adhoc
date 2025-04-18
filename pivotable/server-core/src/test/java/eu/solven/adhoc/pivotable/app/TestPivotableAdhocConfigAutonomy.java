package eu.solven.adhoc.pivotable.app;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.IAdhocQueryEngine;
import eu.solven.adhoc.pivotable.spring.AdhocAutoConfiguration;
import lombok.extern.slf4j.Slf4j;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = { AdhocAutoConfiguration.class })
@Slf4j
public class TestPivotableAdhocConfigAutonomy {

	@Autowired
	ApplicationContext appContest;

	@Test
	public void testEngineEventBus() {
		Assertions.assertThat(appContest.getBean(IAdhocQueryEngine.class))
				.isInstanceOfSatisfying(AdhocQueryEngine.class, engine -> {
					// TODO How to check make a Guava EventBus by default?
					// Assertions.assertThat(engine.getEventBus().getClass().getName()).contains("aaaa");
					Assertions.assertThat(engine.getEventBus()).isNotNull();
				});
	}
}
