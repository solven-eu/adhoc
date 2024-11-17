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
import eu.solven.adhoc.query.AdhocQueryBuilder;
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
		dag.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyingNames(Arrays.asList("k1", "k2"))
				.combinationKey(ExpressionCombination.KEY)
				.options(ImmutableMap.<String, Object>builder().put("expression", "k1 + k2").build())
				.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = aqe.execute(AdhocQueryBuilder.measure("sumK1K2").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 0L + 123 + 234 + 345 + 456));
	}

	@Test
	public void testSumOfSum_oneIsNull_improperFormula() {
		dag.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyingNames(Arrays.asList("k1", "k2"))
				.combinationKey(ExpressionCombination.KEY)
				.options(ImmutableMap.<String, Object>builder().put("expression", "k1 + k2").build())
				.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		// Reject rows where k2 is not null
		ITabularView output = aqe.execute(AdhocQueryBuilder.measure("sumK1K2")
				.andFilter(ColumnFilter.builder().column("k2").matchNull().build())
				.build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", "123.0null"));
	}

	@Test
	public void testSumOfSum_oneIsNull() {
		dag.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyingNames(Arrays.asList("k1", "k2"))
				.combinationKey(ExpressionCombination.KEY)
				// https://github.com/ezylang/EvalEx/issues/204
				// We may process ternary into IF
				// "k1 == null ? 0 : k1 + k2 == null ? 0 : k2"
				.options(ImmutableMap.<String, Object>builder()
						.put("expression", "IF(k1 == null, 0, k1) + IF(k2 == null, 0, k2)")
						.build())
				.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		// Reject rows where k2 is not null
		ITabularView output = aqe.execute(AdhocQueryBuilder.measure("sumK1K2")
				.andFilter(ColumnFilter.builder().column("k2").matchNull().build())
				.build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 123L));
	}
}
