/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.measure.transformator;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.combination.ExpressionCombination;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.query.cube.AdhocQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;

public class TestTransformator_ExpressionCombination extends ADagTest implements IAdhocTestConstants {

	@Override
	@BeforeEach
	public void feedTable() {
		table.add(Map.of("k1", 123D));
		table.add(Map.of("k2", 234D));
		table.add(Map.of("k1", 345D, "k2", 456D));
	}

	@Test
	public void testSumOfSum() {
		forest.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(ExpressionCombination.KEY)
				.combinationOptions(ImmutableMap.<String, Object>builder().put("expression", "k1 + k2").build())
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube.execute(AdhocQuery.builder().measure("sumK1K2").debug(true).build());

		Assertions.assertThat(MapBasedTabularView.load(output).getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 0L + 123 + 234 + 345 + 456));
	}

	@Test
	public void testSumOfSum_oneIsNull_improperFormula() {
		forest.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(ExpressionCombination.KEY)
				.combinationOptions(ImmutableMap.<String, Object>builder().put("expression", "k1 + k2").build())
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		// Reject rows where k2 is not null
		ITabularView output = cube.execute(AdhocQuery.builder().measure("sumK1K2").andFilter("k2", null).build());

		Assertions.assertThat(MapBasedTabularView.load(output).getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", "123.0null"));
	}

	@Test
	public void testSumOfSum_oneIsNull() {
		forest.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(ExpressionCombination.KEY)
				// https://github.com/ezylang/EvalEx/issues/204
				// We may process ternary into IF
				// "k1 == null ? 0 : k1 + k2 == null ? 0 : k2"
				.combinationOptions(ImmutableMap.<String, Object>builder()
						.put("expression", "IF(k1 == null, 0, k1) + IF(k2 == null, 0, k2)")
						.build())
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		// Reject rows where k2 is not null
		ITabularView output = cube.execute(AdhocQuery.builder()
				.measure("sumK1K2")
				.andFilter(ColumnFilter.builder().column("k2").matchNull().build())
				.build());

		Assertions.assertThat(MapBasedTabularView.load(output).getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 123L));
	}

	@Test
	public void testReferenceByIndex() {
		forest.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(ExpressionCombination.KEY)
				// https://github.com/ezylang/EvalEx/issues/204
				// We may process ternary into IF
				// "k1 == null ? 0 : k1 + k2 == null ? 0 : k2"
				.combinationOptions(ImmutableMap.<String, Object>builder()
						.put("expression",
								"IF(underlyings[0] == null, 0, underlyings[0]) + IF(underlyings[1] == null, 0, underlyings[1])")
						.build())
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		// Reject rows where k2 is not null
		ITabularView output = cube.execute(AdhocQuery.builder().measure("sumK1K2").build());

		Assertions.assertThat(MapBasedTabularView.load(output).getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 0L + 123 + 234 + 345 + 456));
	}
}
