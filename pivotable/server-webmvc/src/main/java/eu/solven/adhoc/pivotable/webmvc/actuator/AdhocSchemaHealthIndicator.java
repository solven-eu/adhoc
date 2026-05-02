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
package eu.solven.adhoc.pivotable.webmvc.actuator;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.springframework.boot.health.contributor.AbstractHealthIndicator;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.Health.Builder;
import org.springframework.boot.health.contributor.HealthContributors;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

import eu.solven.adhoc.beta.schema.IAdhocSchema;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.ICubeWrapper;
import lombok.RequiredArgsConstructor;

/**
 * Example of additional health indicator.
 *
 * @author Benoit Lacelle
 */
// https://www.baeldung.com/spring-boot-actuators
@Component
@RequiredArgsConstructor
public class AdhocSchemaHealthIndicator implements CompositeHealthContributor {

	final IAdhocSchema schema;

	@Override
	public HealthIndicator getContributor(String name) {
		return new AbstractHealthIndicator() {
			@Override
			protected void doHealthCheck(Builder builder) throws Exception {
				Optional<ICubeWrapper> optCube =
						schema.getCubes().stream().filter(c -> c.getName().equals(name)).findFirst();
				if (optCube.isEmpty()) {
					builder.unknown();
				} else {
					ICubeWrapper cube = optCube.orElseThrow();

					Map<String, Object> details = CubeWrapper.makeDetails(cube);

					builder.up().withDetails(details);
				}
			}
		};
	}

	@Override
	public Stream<HealthContributors.Entry> stream() {
		return schema.getCubes()
				.stream()
				.map(ICubeWrapper::getName)
				.map(cubeName -> new HealthContributors.Entry(cubeName, getContributor(cubeName)));
	}
}