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
