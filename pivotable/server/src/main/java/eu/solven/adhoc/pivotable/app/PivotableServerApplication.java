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

import java.util.concurrent.ConcurrentHashMap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.session.ReactiveMapSessionRepository;
import org.springframework.session.ReactiveSessionRepository;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.app.example.InjectAdvancedExamplesCubesConfig;
import eu.solven.adhoc.pivotable.app.example.InjectPixarExampleCubesConfig;
import eu.solven.adhoc.pivotable.app.example.InjectSimpleExampleCubesConfig;
import eu.solven.adhoc.pivotable.app.example.InjectWorldCupExampleCubesConfig;
import eu.solven.adhoc.pivotable.core.PivotableComponentsConfiguration;
import eu.solven.adhoc.pivotable.security.PivotableSecuritySpringConfig;
import eu.solven.adhoc.pivotable.webflux.PivotableWebFluxSpringConfig;
import eu.solven.adhoc.tools.GitPropertySourceConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * The main entrypoint for the application.
 * 
 * @author Benoit Lacelle
 */
@SpringBootApplication(scanBasePackages = "none")
@Import({

		PivotableWebFluxSpringConfig.class,
		PivotableComponentsConfiguration.class,
		PivotableSecuritySpringConfig.class,
		GitPropertySourceConfig.class,

		InjectSimpleExampleCubesConfig.class,
		InjectAdvancedExamplesCubesConfig.class,
		InjectPixarExampleCubesConfig.class,
		InjectWorldCupExampleCubesConfig.class,

})
@Slf4j
public class PivotableServerApplication {

	public static void main(String[] args) {
		SpringApplication springApp = new SpringApplication(PivotableServerApplication.class);

		springApp.setAdditionalProfiles(IPivotableSpringProfiles.P_DEFAULT
		// If the dataset is load available on disk, the cube will be automatically skipped
				, IPivotableSpringProfiles.P_ADVANCED_DATASETS);

		springApp.run(args);
	}

	// // https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-3.0-Migration-Guide#spring-session-store-type
	@Bean
	// This will override any auto-configured SessionRepository like Redis one
	@Profile({ IPivotableSpringProfiles.P_INMEMORY })
	public ReactiveSessionRepository<?> inmemorySessionRepository() {
		log.info("Sessions are managed by a {}", ReactiveMapSessionRepository.class.getSimpleName());
		return new ReactiveMapSessionRepository(new ConcurrentHashMap<>());
	}

}
