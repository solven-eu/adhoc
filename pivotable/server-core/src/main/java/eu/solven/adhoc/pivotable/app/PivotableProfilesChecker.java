/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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

import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Profiles;

import com.google.common.base.Strings;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import lombok.extern.slf4j.Slf4j;

/**
 * Checks the spring profiles relative to Pivotable. Typically useful to detect the lack of `spring.config.import:
 * classpath:pivotable-config.yml`
 */
// Loaded by ./src/main/resources/META-INF/spring.factories
@Slf4j
public class PivotableProfilesChecker implements EnvironmentPostProcessor {

	@Override
	public void postProcessEnvironment(ConfigurableEnvironment env, SpringApplication app) {
		String configImport = env.getProperty(IPivotableSpringProfiles.K_CONFIG_IMPORT);

		if (Strings.isNullOrEmpty(configImport)) {
			log.warn("Your `application.yml` (or `application.properties`) is missing {}",
					IPivotableSpringProfiles.P_CONFIG_IMPORT);
		} else if (!configImport.contains(IPivotableSpringProfiles.C_CONFIG)) {
			log.warn("Your `application.yml` (or `application.properties`)  {} given {}={}",
					IPivotableSpringProfiles.C_CONFIG,
					IPivotableSpringProfiles.K_CONFIG_IMPORT,
					configImport);
		} else if (!env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_PIVOTABLE))) {
			log.warn("Conflicting configuration around {}={}", IPivotableSpringProfiles.P_CONFIG_IMPORT, configImport);
		}
	}
}
