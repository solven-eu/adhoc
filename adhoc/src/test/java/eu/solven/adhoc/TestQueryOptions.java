package eu.solven.adhoc;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.aggregations.sum.SumCombination;
import eu.solven.adhoc.query.AdhocQueryBuilder;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Combinator;

public class TestQueryOptions extends ADagTest {
	@BeforeEach
	@Override
	public void feedDb() {
		rows.add(Map.of("k1", 123));
		rows.add(Map.of("k2", 234));
		rows.add(Map.of("k1", 345, "k2", 456));
	}

	@Test
	public void testSumOfSum_() {
		amb.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyingNames(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = aqe.execute(AdhocQueryBuilder.measure("sumK1K2").build(),
				Set.of(StandardQueryOptions.RETURN_UNDERLYING_MEASURES),
				rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.coordinatesToValues)
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of("k1", 0L + 123 + 345, "k2", 0L + 234 + 456, "sumK1K2", 0L + 123 + 234 + 345 + 456));
	}
}
