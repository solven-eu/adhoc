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
import eu.solven.adhoc.aggregations.max.MaxTransformation;
import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.query.AdhocQueryBuilder;
import eu.solven.adhoc.query.GroupByColumns;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Bucketor;

public class TestAggregations_Bucketor extends ADagTest {
	@Override
	@BeforeEach
	public void feedDb() {
		rows.add(Map.of("a", "a1", "k1", 123));
		rows.add(Map.of("a", "a1", "k1", 345, "k2", 456));
		rows.add(Map.of("a", "a2", "b", "b1", "k2", 234));
		rows.add(Map.of("a", "a2", "b", "b2", "k1", 567));
	}

	@Test
	public void testSumOfMaxOfSum_noGroupBy() {
		amb.addMeasure(Bucketor.builder()
				.name("maxK1K2")
				.underlyingNames(Arrays.asList("k1", "k2"))
				.combinatorKey(MaxTransformation.KEY)
				.aggregationKey(SumAggregator.KEY)
				.build());

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = aqe.execute(AdhocQueryBuilder.measure("maxK1K2").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("maxK1K2", 0L + (123 + 345) + 567));
	}

	@Test
	public void testSumOfMaxOfSum_groupByA() {
		amb.addMeasure(Bucketor.builder()
				.name("maxK1K2")
				.underlyingNames(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.of("a"))
				.combinatorKey(MaxTransformation.KEY)
				.aggregationKey(SumAggregator.KEY)
				.build());

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = aqe.execute(AdhocQueryBuilder.measure("maxK1K2").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("maxK1K2", 0L + (123 + 345) + 567));
	}

	@Test
	public void testSumOfMaxOfSum_groupByAandB() {
		amb.addMeasure(Bucketor.builder()
				.name("maxK1K2")
				.underlyingNames(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.of("a", "b"))
				.combinatorKey(MaxTransformation.KEY)
				.aggregationKey(SumAggregator.KEY)
				.build());

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = aqe.execute(AdhocQueryBuilder.measure("maxK1K2").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("maxK1K2", 0L + 234 + 567));
	}

	@Test
	public void testSumOfMaxOfSum_groupByA_bothBucketorAndAdhoc() {
		amb.addMeasure(Bucketor.builder()
				.name("maxK1K2")
				.underlyingNames(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.of("a"))
				.combinatorKey(MaxTransformation.KEY)
				.aggregationKey(SumAggregator.KEY)
				.build());

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = aqe.execute(AdhocQueryBuilder.measure("maxK1K2").addGroupby("a").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(2).contains(Map.of("a", "a1"), Map.of("a", "a2"));

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("a", "a1"), Map.of("maxK1K2", 0L + 123 + 345))
				.containsEntry(Map.of("a", "a2"), Map.of("maxK1K2", 0L + 567));
	}

	@Test
	public void testSumOfMaxOfSum_groupByAandBandUnknown() {
		amb.addMeasure(Bucketor.builder()
				.name("maxK1K2")
				.underlyingNames(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.of("a", "b", "unknown"))
				.combinatorKey(MaxTransformation.KEY)
				.aggregationKey(SumAggregator.KEY)
				.build());

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = aqe.execute(AdhocQueryBuilder.measure("maxK1K2").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(0);

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(0);
	}

	@Test
	public void testSumOfSum_filterA1_groupbyA() {
		amb.addMeasure(Bucketor.builder()
				.name("maxK1K2")
				.underlyingNames(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.of("a"))
				.combinatorKey(MaxTransformation.KEY)
				.aggregationKey(SumAggregator.KEY)
				.build());

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = aqe.execute(AdhocQueryBuilder.measure("maxK1K2").addFilter("a", "a1").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Map.of());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of("maxK1K2", 0L + 123 + 345));
	}

	@Test
	public void testSumOfSum_filterA1_groupbyB() {
		amb.addMeasure(Bucketor.builder()
				.name("maxK1K2")
				.underlyingNames(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.of("b"))
				.combinatorKey(MaxTransformation.KEY)
				.aggregationKey(SumAggregator.KEY)
				.tag("debug")
				.build());

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = aqe.execute(AdhocQueryBuilder.measure("maxK1K2").addFilter("a", "a2").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Map.of());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of("maxK1K2", 0L + 234 + 567));
	}

}
