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

import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.ICubeQueryEngine;
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

		Assertions.assertThat(appContest.getBean(ICubeQueryEngine.class))
				.isInstanceOfSatisfying(CubeQueryEngine.class, engine -> {
					// Ensure we injected properly the custom eventBus
					Assertions.assertThat(engine.getEventBus()).isSameAs(springAdhocEventBus);
				});
	}
}
