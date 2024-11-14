package eu.solven.adhoc;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.aggregations.DivideTransformation;
import eu.solven.adhoc.aggregations.SumAggregator;
import eu.solven.adhoc.api.v1.pojo.EqualsFilter;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.AdhocQueryBuilder;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Combinator;
import eu.solven.adhoc.transformers.Filtrator;

public class TestAggregations_Filtrator extends ADagTest {

	@Override
	@BeforeEach
	public void feedDb() {
		rows.add(Map.of("a", "a1", "k1", 123));
		rows.add(Map.of("a", "a2", "b", "b1", "k2", 234));
		rows.add(Map.of("a", "a1", "k1", 345, "k2", 456));
		rows.add(Map.of("a", "a2", "b", "b2", "k1", 567));
	}

	@Test
	public void testSumOfSum_filterA1() {
		dag

				.addMeasure(Filtrator.builder()
						.name("filterK1onA1")
						.underlyingMeasure("k1")
						.filter(EqualsFilter.builder().axis("a").filtered("a1").build())
						.build())

				.addMeasure(Filtrator.builder()
						.name("filterK1onA2")
						.underlyingMeasure("k1")
						.filter(EqualsFilter.builder().axis("a").filtered("a2").build())
						.build())

				.addMeasure(Combinator.builder()
						.name("Ratio_k1_k1witha1")
						.underlyingNames(Arrays.asList("k1", "filterK1onA1"))
						.transformationKey(DivideTransformation.KEY)
						.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		AdhocQuery adhocQuery = AdhocQueryBuilder.measure("k1").addMeasures("filterK1onA1").build();
		ITabularView output = dag.execute(adhocQuery, rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.coordinatesToValues)
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of("k1", 0L + 123 + 345 + 567, "filterK1onA1", 0L + 123 + 345));
	}

	@Test
	public void testSumOfSum_filterA1_divide() {
		dag

				.addMeasure(Filtrator.builder()
						.name("filterK1onA1")
						.underlyingMeasure("k1")
						.filter(EqualsFilter.builder().axis("a").filtered("a1").build())
						.build())

				.addMeasure(Filtrator.builder()
						.name("filterK1onA2")
						.underlyingMeasure("k1")
						.filter(EqualsFilter.builder().axis("a").filtered("a2").build())
						.build())

				.addMeasure(Combinator.builder()
						.name("Ratio_k1_k1witha1")
						.underlyingNames(Arrays.asList("filterK1onA1", "k1"))
						.transformationKey(DivideTransformation.KEY)
						.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		AdhocQuery adhocQuery = AdhocQueryBuilder.measure("Ratio_k1_k1witha1").build();
		ITabularView output = dag.execute(adhocQuery, rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.coordinatesToValues)
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of("Ratio_k1_k1witha1", 1D * (0L + 123 + 345) / (0L + 123 + 345 + 567)));
	}
}
