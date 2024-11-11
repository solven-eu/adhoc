package eu.solven.adhoc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.aggregations.SumAggregator;
import eu.solven.adhoc.aggregations.SumTransformation;
import eu.solven.adhoc.query.AdhocQueryBuilder;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Combinator;

public class TestAggregations_GroupBys_2Columns  extends ADagTest{
	List<Map<String, ?>> rows = new ArrayList<>();

	@BeforeEach
	public void feedDb() {
		rows.add(Map.of("a", "a1", "k1", 123));
		rows.add(Map.of("a", "a2", "b", "b1", "k2", 234));
		rows.add(Map.of("a", "a1", "k1", 345, "k2", 456));
		rows.add(Map.of("a", "a2", "b", "b2", "k1", 567));
	}

	@Test
	public void testSumOfSum_groupBy2String() {
		dag.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyingMeasures(Arrays.asList("k1", "k2"))
				.transformationKey(SumTransformation.KEY)
				.build());

		dag.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		dag.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());

		ITabularView output = dag.execute(AdhocQueryBuilder.measure("sumK1K2").addGroupby("a").addGroupby("b").build(),
				rows.stream());

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(2).contains(Map.of("a", "a2", "b", "b2"), Map.of("a", "a2", "b", "b1"));

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.coordinatesToValues)
				.hasSize(2)
				.containsEntry(Map.of("a", "a2", "b", "b1"), Map.of("sumK1K2", 0L + 234))
				.containsEntry(Map.of("a", "a2", "b", "b2"), Map.of("sumK1K2", 0L + 567));
	}

}
