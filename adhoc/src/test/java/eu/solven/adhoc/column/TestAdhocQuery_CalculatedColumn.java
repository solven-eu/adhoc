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
package eu.solven.adhoc.column;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.cube.CubeQuery;

public class TestAdhocQuery_CalculatedColumn extends ADagTest implements IAdhocTestConstants {
	@Override
	@BeforeEach
	public void feedTable() {
		table.add(Map.of("a", "a1", "k1", 123));
		table.add(Map.of("a", "a1", "k1", 345, "k2", 456));
		table.add(Map.of("a", "a2", "b", "b1", "k2", 234));
		table.add(Map.of("a", "a2", "b", "b2", "k1", 567));

		// This first `k1` overlaps with the columnName
		forest.addMeasure(Aggregator.builder().name("k1").columnName("k1").aggregationKey(SumAggregation.KEY).build());
		// This second `k1.SUM` does not overlap with the columnName
		forest.addMeasure(
				Aggregator.builder().name("k1.sum").columnName("k1").aggregationKey(SumAggregation.KEY).build());
	}

	@Test
	public void test_groupBy() {
		ITabularView view = cube.execute(CubeQuery.builder()
				.measure("k1")
				.groupByAlso(CalculatedColumn.builder()
						.name("custom")
						.recordToCoordinate(r -> r.getGroupBy("a") + "-" + r.getGroupBy("b"))
						.build())
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(view);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("custom", "a1-null"), Map.of("k1", 0L + 123 + 345))
				.containsEntry(Map.of("custom", "a2-b2"), Map.of("k1", 0L + 567))
				.hasSize(2);
	}
}
