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
package eu.solven.adhoc.pivotable.webnone.api.security;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.webnone.security.PivotableSecurityWebnoneSpringConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Tests for the profile-consistency guards in {@link PivotableSecurityWebnoneSpringConfig}.
 *
 * <p>
 * The {@code server} module has no web stack, so HTTP-level security assertions are covered by
 * {@code TestPivotableJwtWebmvcSecurity} in the {@code server-webmvc} module. This class focuses on the non-HTTP
 * behaviour: verifying that incompatible profile combinations are rejected at startup.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class TestPivotableSecurityWebnoneSpringConfig {

	/**
	 * The application context must start successfully when the {@code P_UNSAFE} profile is active (no profile-conflict
	 * check fails, and {@code checkSecured} is only active under {@code P_PRDMODE}).
	 */
	@Test
	public void testContext_loadsWithUnsafeProfile() {
		try (ConfigurableApplicationContext ctx = SpringApplication.run(PivotableServerSecurityWebnoneApplication.class,
				"--spring.profiles.active=" + IPivotableSpringProfiles.P_UNSAFE,
				"--spring.config.import=classpath:pivotable-config.yml")) {
			Assertions.assertThat(ctx.isActive()).isTrue();
		}
	}

	/**
	 * Activating both {@code P_INMEMORY} and {@code P_REDIS} simultaneously must be rejected at context startup via the
	 * {@code checkSpringProfilesConsistency} bean in {@link PivotableSecurityWebnoneSpringConfig}.
	 */
	@Test
	public void testContext_rejectsIncompatibleProfiles_inmemoryAndRedis() {
		Assertions
				.assertThatThrownBy(() -> SpringApplication.run(PivotableServerSecurityWebnoneApplication.class,
						"--spring.profiles.active=" + IPivotableSpringProfiles.P_UNSAFE
								+ ","
								+ IPivotableSpringProfiles.P_INMEMORY
								+ ","
								+ IPivotableSpringProfiles.P_REDIS,
						"--spring.config.import=classpath:pivotable-config.yml"))
				.isInstanceOf(Exception.class)
				.satisfies(e -> {
					// The real cause is wrapped in BeanCreationException chains
					Throwable root = e;
					while (root.getCause() != null && !(root instanceof IllegalStateException)) {
						root = root.getCause();
					}
					Assertions.assertThat(root)
							.isInstanceOf(IllegalStateException.class)
							.hasMessageContaining(IPivotableSpringProfiles.P_INMEMORY);
				});
	}
}
