package eu.solven.adhoc.measure;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.ITabularView;
import eu.solven.adhoc.MapBasedTabularView;
import eu.solven.adhoc.aggregations.MaxAggregator;
import eu.solven.adhoc.aggregations.MaxTransformation;
import eu.solven.adhoc.aggregations.SumAggregator;
import eu.solven.adhoc.aggregations.SumTransformation;
import eu.solven.adhoc.query.AdhocQueryBuilder;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Combinator;

public class TestAggregations_Double extends ADagTest {
	@Override
	@BeforeEach
	public void feedDb() {
		rows.add(Map.of("k1", 123D));
		rows.add(Map.of("k2", 234D));
		rows.add(Map.of("k1", 345F, "k2", 456F));
	}

	@Test
	public void testSumOfSum() {
		dag.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyingNames(Arrays.asList("k1", "k2"))
				.transformationKey(SumTransformation.KEY)
				.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = dag.execute(AdhocQueryBuilder.measure("sumK1K2").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						// "k1", 123 + 345, "k2", 234 + 456,
						Map.of("sumK1K2", 0D + 123 + 234 + 345 + 456));
	}

	@Test
	public void testSumOfMax() {
		dag.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyingNames(Arrays.asList("k1", "k2"))
				.transformationKey(SumTransformation.KEY)
				.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(MaxAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(MaxAggregator.KEY).build());

		ITabularView output = dag.execute(AdhocQueryBuilder.measure("sumK1K2").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						// "k1", 345, "k2", 456,
						Map.of("sumK1K2", 0D + 345 + 456));
	}

	@Test
	public void testMaxOfSum() {
		dag.addMeasure(Combinator.builder()
				.name("maxK1K2")
				.underlyingNames(Arrays.asList("k1", "k2"))
				.transformationKey(MaxTransformation.KEY)
				.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = dag.execute(AdhocQueryBuilder.measure("maxK1K2").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						// "k1", 123 + 345, "k2", 234 + 456,
						Map.of("maxK1K2", 0D + 234 + 456));
	}
}
