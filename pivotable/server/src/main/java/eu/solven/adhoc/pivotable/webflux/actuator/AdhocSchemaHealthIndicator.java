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
package eu.solven.adhoc.pivotable.webflux.actuator;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import org.springframework.boot.actuate.health.AbstractReactiveHealthIndicator;
import org.springframework.boot.actuate.health.CompositeReactiveHealthContributor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.NamedContributor;
import org.springframework.boot.actuate.health.ReactiveHealthContributor;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.stereotype.Component;

import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.ICubeWrapper;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

/**
 * Example of additional health indicator.
 *
 * @author Benoit Lacelle
 */
// https://www.baeldung.com/spring-boot-actuators
@Component
@RequiredArgsConstructor
public class AdhocSchemaHealthIndicator implements CompositeReactiveHealthContributor {

	final AdhocSchema schema;

	@Override
	public ReactiveHealthIndicator getContributor(String name) {
		return new AbstractReactiveHealthIndicator() {

			@Override
			protected Mono<Health> doHealthCheck(Health.Builder builder) {
				Optional<ICubeWrapper> optCube =
						schema.getCubes().stream().filter(c -> c.getName().equals(name)).findFirst();
				if (optCube.isEmpty()) {
					return Mono.just(Health.unknown().build());
				}

				ICubeWrapper cube = optCube.orElseThrow();

				Map<String, Object> details = CubeWrapper.makeDetails(cube);

				return Mono.just(Health.up().withDetails(details).build());
			}

		};
	}

	@Override
	public Iterator<NamedContributor<ReactiveHealthContributor>> iterator() {
		return schema.getCubes()
				.stream()
				.map(c -> c.getName())
				.<NamedContributor<ReactiveHealthContributor>>map(
						cubeName -> NamedContributor.of(cubeName, getContributor(cubeName)))
				.iterator();
	}
}