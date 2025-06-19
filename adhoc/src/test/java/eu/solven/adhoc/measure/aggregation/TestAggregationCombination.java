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
package eu.solven.adhoc.measure.aggregation;

import java.util.Arrays;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.sum.AggregationCombination;

public class TestAggregationCombination {
	@Test
	public void testFromMap_noCustomOnNull() {
		Map<String, ?> options = ImmutableMap.<String, Object>builder()
				.put(AggregationCombination.K_AGGREGATION_KEY, MaxAggregation.KEY)
				.put(AggregationCombination.K_AGGREGATION_OPTIONS, Map.of())
				.build();
		AggregationCombination agg = new AggregationCombination(options);

		Assertions.assertThat(agg.combine(null, Arrays.asList("abc", "def"))).isEqualTo("def");
		Assertions.assertThat(agg.combine(null, Arrays.asList(123, -234))).isEqualTo(123L);
		Assertions.assertThat(agg.combine(null, Arrays.asList(12.34D, null, 34.56D))).isEqualTo(34.56D);
	}

	@Test
	public void testFromMap_customOnNull() {
		Map<String, ?> options = ImmutableMap.<String, Object>builder()
				.put(AggregationCombination.K_AGGREGATION_KEY, MaxAggregation.KEY)
				.put(AggregationCombination.K_AGGREGATION_OPTIONS, Map.of())
				.put(AggregationCombination.K_CUSTOM_IF_ANY_NULL_OPERAND, true)
				.build();
		AggregationCombination agg = new AggregationCombination(options);

		Assertions.assertThat(agg.combine(null, Arrays.asList("abc", "def"))).isEqualTo("def");
		Assertions.assertThat(agg.combine(null, Arrays.asList(123, -234))).isEqualTo(123L);
		Assertions.assertThat(agg.combine(null, Arrays.asList(12.34D, null, 34.56D))).isEqualTo(null);
	}
}
