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
package eu.solven.adhoc.cube.training.b_easy;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.forest.MeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.table.InMemoryTable;

public class HelloAggregations {
	// Create your own aggregation function
	public static final class ConcatAggregation implements IAggregation {
		@Override
		public Object aggregate(Object left, Object right) {
			if (left == null && right == null) {
				// For any reason, both were null
				return null;
			} else if (left == null) {
				// Ensure type consistency
				return right.toString();
			} else if (right == null) {
				// Ensure type consistency
				return left.toString();
			} else {
				// Do the concatenation
				return left.toString() + "-" + right.toString();
			}
		}
	}

	@Test
	public void helloCustomAggregation() {
		InMemoryTable table = InMemoryTable.builder().build();
		table.add(ImmutableMap.of("a", "a1", "b", "b1", "v1", 1, "v2", 2));
		table.add(ImmutableMap.of("a", "a1", "b", "b2", "v2", 7));
		table.add(ImmutableMap.of("a", "a2", "b", "b1", "v1", 11));

		MeasureForest forest = MeasureForest.builder()
				.name("someForest")
				.measure(Aggregator.builder()
						.name("v1")
						.aggregationKey(ConcatAggregation.class.getName())
						.columnName("v1")
						.build())
				.measure(Aggregator.builder()
						.name("v2")
						.aggregationKey(ConcatAggregation.class.getName())
						.columnName("v2")
						.build())
				.build();

		ICubeWrapper cube = CubeWrapper.builder().table(table).forest(forest).build();

		ISliceFilter customFilter = ColumnFilter.match("a", someA -> {
			return someA instanceof String someAString && someAString.endsWith("1");
		});

		ICubeQuery query = CubeQuery.builder().groupByAlso("a").measure("v1", "v2").filter(customFilter).build();

		ITabularView result = cube.execute(query);
		MapBasedTabularView resultListBased = MapBasedTabularView.load(result);

		Assertions.assertThat(resultListBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of("a", "a1"), Map.of("v1", "1", "v2", "2-7"));
	}
}
