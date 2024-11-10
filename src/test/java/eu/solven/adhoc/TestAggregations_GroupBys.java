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

public class TestAggregations_GroupBys {
	EventBus eventBus = new EventBus();
	AdhocEventsToSfl4j toSlf4j = new AdhocEventsToSfl4j();
	DAG dag = new DAG(eventBus);
	List<Map<String, ?>> rows = new ArrayList<>();

	@BeforeEach
	public void wireEvents() {
		eventBus.register(toSlf4j);

		rows.add(Map.of("a", "a1", "k1", 123));
		rows.add(Map.of("a", "a2", "b", "b1", "k2", 234));
		rows.add(Map.of("a", "a1", "k1", 345, "k2", 456));
		rows.add(Map.of("a", "a2", "b", "b2", "k1", 567));
	}

	@Test
	public void testSumOfSum_noGroupBy() {
		dag.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyingMeasures(Arrays.asList("k1", "k2"))
				.transformationKey(SumTransformation.KEY)
				.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = dag.execute(AdhocQueryBuilder.measure("sumK1K2").build(), rows.stream());

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.coordinatesToValues)
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						// "k1", 123 + 345, "k2", 234 + 456,
						Map.of("sumK1K2", 0L + 123 + 234 + 345 + 456 + 567));
	}

	@Test
	public void testSumOfSum_groupBy1String() {
		dag.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyingMeasures(Arrays.asList("k1", "k2"))
				.transformationKey(SumTransformation.KEY)
				.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = dag.execute(AdhocQueryBuilder.measure("sumK1K2").addGroupby("a").build(), rows.stream());

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(2).contains(Map.of("a", "a1"), Map.of("a", "a2"));

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.coordinatesToValues)
				.hasSize(2)
				.containsEntry(Map.of("a", "a1"), Map.of("sumK1K2", 0L + 234))
				.containsEntry(Map.of("a", "a2"), Map.of("sumK1K2", 0L + 123 + 345 + 456));
	}

	@Test
	public void testSumOfSum_groupBy1String_notAlwaysPresent() {
		dag.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyingMeasures(Arrays.asList("k1", "k2"))
				.transformationKey(SumTransformation.KEY)
				.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = dag.execute(AdhocQueryBuilder.measure("sumK1K2").addGroupby("b").build(), rows.stream());

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(2).contains(Map.of("b", "b1"));

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.coordinatesToValues)
				.hasSize(1)
				.containsEntry(Map.of("b", "b1"), Map.of("sumK1K2", 0L + 234));
	}

	@Test
	public void testSumOfMax_groupBy1String() {
		dag.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyingMeasures(Arrays.asList("k1", "k2"))
				.transformationKey(SumTransformation.KEY)
				.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(MaxAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(MaxAggregator.KEY).build());

		ITabularView output = dag.execute(AdhocQueryBuilder.measure("sumK1K2").addGroupby("a").build(), rows.stream());

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.coordinatesToValues)
				.hasSize(1)
				.containsEntry(Map.of("a", "a1"), Map.of("sumK1K2", 0L + 345))
				.containsEntry(Map.of("a", "a2"), Map.of("sumK1K2", 0L + 234));
	}

	@Test
	public void testMaxOfSum() {
		dag.addMeasure(Combinator.builder()
				.name("maxK1K2")
				.underlyingMeasures(Arrays.asList("k1", "k2"))
				.transformationKey(MaxTransformation.KEY)
				.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = dag.execute(AdhocQueryBuilder.measure("maxK1K2").addGroupby("a").build(), rows.stream());

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.coordinatesToValues)
				.hasSize(2)
				.containsEntry(Map.of("a", "a1"), Map.of("sumK1K2", 0L + 345))
				.containsEntry(Map.of("a", "a2"), Map.of("sumK1K2", 0L + 234));
	}
}
