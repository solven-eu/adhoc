package eu.solven.adhoc.pivotable.app;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.IAdhocQueryEngine;
import eu.solven.adhoc.pivotable.account.IAdhocUserRawRawRepository;
import eu.solven.adhoc.pivotable.account.IAdhocUserRepository;
import eu.solven.adhoc.pivotable.core.PivotableComponentsConfiguration;
import eu.solven.adhoc.util.IAdhocEventBus;
import lombok.extern.slf4j.Slf4j;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = { PivotableComponentsConfiguration.class,

// Keep this commented to check `@EnableAutoConfiguration`
// AdhocAutoConfiguration.class,

})
@Slf4j
@MockitoBean(types = { IAdhocUserRepository.class, IAdhocUserRawRawRepository.class })
@EnableAutoConfiguration
public class TestPivotableComponentsConfiguration {

	@Autowired
	ApplicationContext appContest;

	@Test
	public void testSession() {
		IAdhocEventBus springAdhocEventBus = appContest.getBean(IAdhocEventBus.class);

		Assertions.assertThat(appContest.getBean(IAdhocQueryEngine.class))
				.isInstanceOfSatisfying(AdhocQueryEngine.class, engine -> {
					// Ensure we injected properly the custom eventBus
					Assertions.assertThat(engine.getEventBus()).isSameAs(springAdhocEventBus);
				});
	}
}
