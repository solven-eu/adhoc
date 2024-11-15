package eu.solven.adhoc;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.aggregations.SumAggregator;
import eu.solven.adhoc.query.AdhocQueryBuilder;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Dispatchor;

public class TestAggregations_Dispatchor extends ADagTest {
	@Override
	@BeforeEach
	public void feedDb() {
		rows.add(Map.of("a", "a1", "percent", 001, "k1", 123));
		rows.add(Map.of("a", "a1", "percent", 10, "k1", 345, "k2", 456));
		rows.add(Map.of("a", "a2", "percent", 100, "b", "b1", "k2", 234));
		rows.add(Map.of("a", "a2", "percent", 50, "b", "b2", "k1", 567));
	}

	@Test
	public void testSumOfMaxOfSum_identity() {
		dag.addMeasure(
				Dispatchor.builder().name("0or100").underlyingMeasure("k1").aggregationKey(SumAggregator.KEY).build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = dag.execute(AdhocQueryBuilder.measure("0or100").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.coordinatesToValues)
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("0or100", 0L + (123 + 345) + 567));
	}

	@Test
	public void testSumOfMaxOfSum_0to100_inputNotRequested() {
		dag.addMeasure(Dispatchor.builder()
				.name("0or100")
				.underlyingMeasure("k1")
				.decompositionKey("linear")
				.decompositionOptions(Map.of("input", "percent", "min", 0, "max", 100, "output", "0_or_100"))
				.aggregationKey(SumAggregator.KEY)
				.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = dag.execute(AdhocQueryBuilder.measure("0or100").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.coordinatesToValues)
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("0or100", 0L + (123 + 345) + 567));
	}

	@Test
	public void testSumOfMaxOfSum_0to100() {
		dag.addMeasure(Dispatchor.builder()
				.name("0or100")
				.underlyingMeasure("k1")
				// .combinatorKey(MaxTransformation.KEY)
				.decompositionKey("linear")
				.decompositionOptions(Map.of("input", "percent", "min", 0, "max", 100, "output", "0_or_100"))
				.aggregationKey(SumAggregator.KEY)
				.debug(true)
				.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output =
				dag.execute(AdhocQueryBuilder.measure("0or100").addGroupby("0_or_100").explain(true).build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(2).contains(Map.of("0_or_100", 0), Map.of("0_or_100", 100));

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.coordinatesToValues)
				.hasSize(2)
				.containsEntry(Map.of("0_or_100", 0), Map.of("0or100", 0D + 123 * 0.01D + 345 * .1D + 567 * 0.5D))
				.containsEntry(Map.of("0_or_100", 100), Map.of("0or100", 0D + 123 * 0.99D + 345 * 0.9D + 567 * 0.5D));
	}

}
