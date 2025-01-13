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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.ITabularView;
import eu.solven.adhoc.MapBasedTabularView;
import eu.solven.adhoc.aggregations.ExpressionCombination;
import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.api.v1.pojo.ColumnFilter;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.slice.AdhocSliceAsMap;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Combinator;

public class TestAggregations_ExpressionTransformation extends ADagTest {

	@Override
	@BeforeEach
	public void feedDb() {
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

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = aqe.execute(AdhocQuery.builder().measure("sumK1K2").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
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

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		// Reject rows where k2 is not null
		ITabularView output = aqe.execute(AdhocQuery.builder()
				.measure("sumK1K2")
				.andFilter(ColumnFilter.builder().column("k2").matchNull().build())
				.build(), rows);

		List<Map<String, ?>> keySet = output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

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

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		// Reject rows where k2 is not null
		ITabularView output = aqe.execute(AdhocQuery.builder()
				.measure("sumK1K2")
				.andFilter(ColumnFilter.builder().column("k2").matchNull().build())
				.build(), rows);

		List<Map<String, ?>> keySet = output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 123L));
	}
}
