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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.session.ReactiveMapSessionRepository;
import org.springframework.session.ReactiveSessionRepository;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import lombok.extern.slf4j.Slf4j;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = { PivotableServerApplication.class },
		webEnvironment = SpringBootTest.WebEnvironment.MOCK,
		properties = {
				// config has to be setup is a very root location, but we have no application.yml to stand as a library
				"spring.config.import=" + IPivotableSpringProfiles.C_CONFIG,
				"spring.profiles.active" + "=" + IPivotableSpringProfiles.P_UNSAFE })
@Slf4j
public class TestSpringAutonomyUnsafe implements IPivotableSpringProfiles {

	@Autowired
	ApplicationContext appContext;
	@Autowired
	Environment env;

	@Test
	public void testSpringProfiles() {
		log.info("startupDate: {}", appContext.getStartupDate());

		Assertions.assertThat(env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_INMEMORY))).isTrue();
	}

	@Test
	public void testSession() {
		Assertions.assertThat(appContext.getBean(ReactiveSessionRepository.class))
				.isInstanceOf(ReactiveMapSessionRepository.class);
	}
}
