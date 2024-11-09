package eu.solven.adhoc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.greenrobot.eventbus.EventBus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.aggregations.MaxAggregator;
import eu.solven.adhoc.aggregations.MaxTransformation;
import eu.solven.adhoc.aggregations.SumAggregator;
import eu.solven.adhoc.aggregations.SumTransformation;
import eu.solven.adhoc.dag.DAG;
import eu.solven.adhoc.eventbus.AdhocEventsToSfl4j;
import eu.solven.adhoc.query.AdhocQueryBuilder;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Combinator;

public class TestAggregations_Double {
	EventBus eventBus = new EventBus();
	AdhocEventsToSfl4j toSlf4j = new AdhocEventsToSfl4j();
	DAG dag = new DAG(eventBus);

	@BeforeEach
	public void wireEvents() {
		eventBus.register(toSlf4j);
	}

	@Test
	public void testSumOfSum() {
		dag.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyingMeasurators(Arrays.asList("k1", "k2"))
				.transformationKey(SumTransformation.KEY)
				.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		List<Map<String, ?>> rows = new ArrayList<>();

		rows.add(Map.of("k1", 123D));
		rows.add(Map.of("k2", 234D));
		rows.add(Map.of("k1", 345D, "k2", 456D));

		ITabularView output = dag.execute(AdhocQueryBuilder.measure("sumK1K2").build(), rows.stream());

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.coordinatesToValues)
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						// "k1", 123 + 345, "k2", 234 + 456,
						Map.of("sumK1K2", 0D + 123 + 234 + 345 + 456));
	}

	@Test
	public void testSumOfMax() {
		dag.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyingMeasurators(Arrays.asList("k1", "k2"))
				.transformationKey(SumTransformation.KEY)
				.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(MaxAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(MaxAggregator.KEY).build());

		List<Map<String, ?>> rows = new ArrayList<>();

		rows.add(Map.of("k1", 123D));
		rows.add(Map.of("k2", 234D));
		rows.add(Map.of("k1", 345F, "k2", 456F));

		ITabularView output = dag.execute(AdhocQueryBuilder.measure("sumK1K2").build(), rows.stream());

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.coordinatesToValues)
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						// "k1", 345, "k2", 456,
						Map.of("sumK1K2", 0D + 345 + 456));
	}

	@Test
	public void testMaxOfSum() {
		dag.addMeasure(Combinator.builder()
				.name("maxK1K2")
				.underlyingMeasurators(Arrays.asList("k1", "k2"))
				.transformationKey(MaxTransformation.KEY)
				.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		List<Map<String, ?>> rows = new ArrayList<>();

		rows.add(Map.of("k1", 123D));
		rows.add(Map.of("k2", 234D));
		rows.add(Map.of("k1", 345F, "k2", 456F));

		ITabularView output = dag.execute(AdhocQueryBuilder.measure("maxK1K2").build(), rows.stream());

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.coordinatesToValues)
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						// "k1", 123 + 345, "k2", 234 + 456,
						Map.of("maxK1K2", 0D + 234 + 456));
	}
}
