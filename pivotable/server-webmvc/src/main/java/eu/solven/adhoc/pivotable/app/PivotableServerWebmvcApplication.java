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

import org.springframework.boot.Banner.Mode;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.ApplicationPidFileWriter;
import org.springframework.context.annotation.Import;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.app.example.InjectAdvancedExamplesCubesConfig;
import eu.solven.adhoc.pivotable.app.example.InjectPixarExampleCubesConfig;
import eu.solven.adhoc.pivotable.app.example.InjectSimpleExampleCubesConfig;
import eu.solven.adhoc.pivotable.app.example.InjectWorldCupExampleCubesConfig;
import eu.solven.adhoc.pivotable.app.mvc.actuator.AdhocSchemaHealthIndicator;
import eu.solven.adhoc.pivotable.core.PivotableComponentsConfiguration;
import eu.solven.adhoc.pivotable.webnone.security.PivotableSecurityWebnoneSpringConfig;
import eu.solven.adhoc.table.sql.AdhocJooqHelper;
import eu.solven.adhoc.tools.GitPropertySourceConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * The main entrypoint for the application.
 * 
 * @author Benoit Lacelle
 */
@SpringBootApplication(scanBasePackages = "none")
@Import({

		PivotableWebmvcSpringConfig.class,
		PivotableComponentsConfiguration.class,
		PivotableSecurityWebnoneSpringConfig.class,
		GitPropertySourceConfig.class,

		InjectSimpleExampleCubesConfig.class,
		InjectAdvancedExamplesCubesConfig.class,
		InjectPixarExampleCubesConfig.class,
		InjectWorldCupExampleCubesConfig.class,

		AdhocSchemaHealthIndicator.class, })
@Slf4j
@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class PivotableServerWebmvcApplication {

	public static void main(String[] args) {
		AdhocJooqHelper.disableBanners();

		SpringApplication springApp = new SpringApplicationBuilder(PivotableServerWebmvcApplication.class)
				// A real-project should set this in its application.yml
				// Pivotable does not provide an application.yml to prevent conflicts
				.properties(IPivotableSpringProfiles.P_CONFIG_IMPORT, "spring.devtools.restart.enabled:false")
				.bannerMode(Mode.OFF)
				// https://docs.spring.io/spring-boot/reference/actuator/process-monitoring.html
				.listeners(new ApplicationPidFileWriter())
				.profiles(IPivotableSpringProfiles.P_UNSAFE
				// If the dataset is not available on disk, the cube will be automatically skipped
						, IPivotableSpringProfiles.P_ADVANCED_DATASETS)
				.build();

		springApp.run(args);
	}

}
