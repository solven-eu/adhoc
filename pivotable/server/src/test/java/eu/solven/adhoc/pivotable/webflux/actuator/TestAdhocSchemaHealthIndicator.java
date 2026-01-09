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
package eu.solven.adhoc.pivotable.webflux.actuator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.mock.env.MockEnvironment;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.InMemoryTable;

public class TestAdhocSchemaHealthIndicator {
	AdhocSchema schema = AdhocSchema.builder().env(new MockEnvironment()).build();

	@Test
	public void testEmpty() {
		AdhocSchemaHealthIndicator indicator = new AdhocSchemaHealthIndicator(schema);

		ReactiveHealthIndicator contributor = indicator.getContributor("notExisting");
		Health health = contributor.getHealth(false).block();
		Assertions.assertThat(health.getStatus()).isEqualTo(Status.UNKNOWN);
	}

	@Test
	public void testOne() {
		ITableWrapper table = InMemoryTable.builder().build();
		CubeWrapper cube =
				CubeWrapper.builder().name("someCubeName").table(table).forest(MeasureForest.empty()).build();
		schema.registerCube(cube);

		AdhocSchemaHealthIndicator indicator = new AdhocSchemaHealthIndicator(schema);

		ReactiveHealthIndicator contributor = indicator.getContributor(cube.getName());
		Health health = contributor.getHealth(true).block();
		Assertions.assertThat(health.getStatus()).isEqualTo(Status.UP);
		Assertions.assertThat(health.getDetails())
				.containsEntry("columns", 0)
				.containsEntry("measures", 0)
				.containsEntry("table", ImmutableMap.of("name", "inMemory", "rows", 0))
				.hasSize(3);
	}
}
