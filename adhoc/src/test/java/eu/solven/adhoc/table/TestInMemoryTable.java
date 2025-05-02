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
package eu.solven.adhoc.table;

import java.time.LocalDate;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.CubeQuery;

public class TestInMemoryTable {
	@Test
	public void testColumns() {
		InMemoryTable table = InMemoryTable.builder().build();

		table.add(Map.of("c", "someString", "v", 123));
		table.add(Map.of("c", "someString", "v", 12.34));
		table.add(Map.of("c", "someString", "date", LocalDate.now()));

		Assertions.assertThat(table.getColumnTypes())
				.hasSize(3)
				.containsEntry("c", String.class)
				.containsEntry("v", Number.class)
				.containsEntry("date", LocalDate.class);
	}

	@Test
	public void testFilterUnknownColumn() {
		InMemoryTable table = InMemoryTable.builder().build();

		table.add(Map.of("k", "v"));

		IMeasureForest forest = MeasureForest.builder().name("count").measure(Aggregator.countAsterisk()).build();
		CubeWrapper cube = CubeWrapper.builder().forest(forest).table(table).build();

		Assertions
				.assertThatThrownBy(
						() -> cube.execute(CubeQuery.builder().andFilter("unknownColumn", "anyValue").build()))
				.isInstanceOf(IllegalArgumentException.class)
				.hasStackTraceContaining("unknownColumn");
	}

	@Test
	public void testGroupByUnknownColumn() {
		InMemoryTable table = InMemoryTable.builder().build();

		table.add(Map.of("k", "v"));

		IMeasureForest forest = MeasureForest.builder().name("count").measure(Aggregator.countAsterisk()).build();
		CubeWrapper cube = CubeWrapper.builder().forest(forest).table(table).build();

		Assertions.assertThatThrownBy(() -> cube.execute(CubeQuery.builder().groupByAlso("unknownColumn").build()))
				.isInstanceOf(IllegalArgumentException.class)
				.hasStackTraceContaining("unknownColumn");
	}
}
