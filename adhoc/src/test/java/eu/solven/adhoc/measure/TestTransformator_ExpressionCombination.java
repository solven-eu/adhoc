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
package eu.solven.adhoc.measure;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.measure.combination.ExpressionCombination;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.storage.MapBasedTabularView;

public class TestTransformator_ExpressionCombination extends ADagTest implements IAdhocTestConstants {

	@Override
	@BeforeEach
	public void feedTable() {
		rows.add(Map.of("k1", 123D));
		rows.add(Map.of("k2", 234D));
		rows.add(Map.of("k1", 345D, "k2", 456D));
	}

	@Test
	public void testSumOfSum() {
		amb.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(ExpressionCombination.KEY)
				.combinationOptions(ImmutableMap.<String, Object>builder().put("expression", "k1 + k2").build())
				.build());

		amb.addMeasure(k1Sum);
		amb.addMeasure(k2Sum);

		ITabularView output = aqw.execute(AdhocQuery.builder().measure("sumK1K2").debug(true).build());

		MapBasedTabularView view = MapBasedTabularView.load(output);

		Assertions.assertThat(view.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 0L + 123 + 234 + 345 + 456));
	}

	@Test
	public void testSumOfSum_oneIsNull_improperFormula() {
		amb.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(ExpressionCombination.KEY)
				.combinationOptions(ImmutableMap.<String, Object>builder().put("expression", "k1 + k2").build())
				.build());

		amb.addMeasure(k1Sum);
		amb.addMeasure(k2Sum);

		// Reject rows where k2 is not null
		ITabularView output = aqw.execute(AdhocQuery.builder().measure("sumK1K2").andFilter("k2", null).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", "123.0null"));
	}

	@Test
	public void testSumOfSum_oneIsNull() {
		amb.addMeasure(Combinator.builder()
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

		amb.addMeasure(k1Sum);
		amb.addMeasure(k2Sum);

		// Reject rows where k2 is not null
		ITabularView output = aqw.execute(AdhocQuery.builder()
				.measure("sumK1K2")
				.andFilter(ColumnFilter.builder().column("k2").matchNull().build())
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 123L));
	}
}
