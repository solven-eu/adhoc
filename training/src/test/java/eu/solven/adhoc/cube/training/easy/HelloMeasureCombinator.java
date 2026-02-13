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
package eu.solven.adhoc.cube.training.easy;

import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.table.InMemoryTable;

public class HelloMeasureCombinator {
	public static class SumPositiveCombination implements ICombination {
		@Override
		public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
			return underlyingValues.stream().map(o -> {
				if (o == null) {
					return null;
				} else if (o instanceof Number n) {
					return Math.abs(n.doubleValue());
				} else {
					// received a String? Return as-is
					return null;
				}
			}).filter(d -> d != null).mapToDouble(d -> d).sum();
		}
	}

	@Test
	public void helloCubeQuery() {
		InMemoryTable table = InMemoryTable.builder().build();
		table.add(ImmutableMap.of("a", "a1", "b", "b1", "v1", 1, "v2", 2));
		table.add(ImmutableMap.of("a", "a1", "b", "b2", "v2", -7));
		table.add(ImmutableMap.of("a", "a2", "b", "b1", "v1", -11));

		IMeasure sum = Combinator.sum("v1", "v2");
		MeasureForest forest = MeasureForest.builder()
				.name("someForest")
				.measure(Aggregator.sum("v1"))
				.measure(Aggregator.sum("v2"))
				.measure(sum)
				.build();

		ICubeWrapper cube = CubeWrapper.builder().table(table).forest(forest).build();

		ICubeQuery query = CubeQuery.builder().groupByAlso("a").measure(sum.getName()).andFilter("b", "b1").build();

		ITabularView result = cube.execute(query);
		MapBasedTabularView resultListBased = MapBasedTabularView.load(result);

		Assertions.assertThat(resultListBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("a", "a1"), Map.of(sum.getName(), 1L + 2L))
				.containsEntry(Map.of("a", "a2"), Map.of(sum.getName(), 11L));
	}
}
