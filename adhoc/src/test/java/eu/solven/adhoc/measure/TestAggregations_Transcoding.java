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
import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.aggregations.sum.SumCombination;
import eu.solven.adhoc.database.InMemoryDatabase;
import eu.solven.adhoc.database.PrefixTranscoder;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Combinator;

public class TestAggregations_Transcoding extends ADagTest {
	final public InMemoryDatabase rows =
			InMemoryDatabase.builder().transcoder(PrefixTranscoder.builder().prefix("p_").build()).build();

	@Override
	@BeforeEach
	public void feedDb() {
		// As assume the data in DB is already prefixed with `_p`
		rows.add(Map.of("p_c", "v1", "p_k1", 123D));
		rows.add(Map.of("p_c", "v2", "p_k2", 234D));

		amb.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyingNames(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());
	}

	@Test
	public void testGrandTotal() {
		ITabularView output = aqe.execute(AdhocQuery.builder().measure("sumK1K2").debug(true).build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 0D + 123 + 234));
	}

	@Test
	public void testFilter() {
		ITabularView output = aqe.execute(AdhocQuery.builder().measure("sumK1K2").andFilter("c", "v1").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 0D + 123));
	}

	@Test
	public void testGroupBy() {
		ITabularView output = aqe.execute(AdhocQuery.builder().measure("sumK1K2").groupByColumns("c").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(2).contains(Map.of("c", "c1"), Map.of("c", "c2"));

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("c", "c1"), Map.of("sumK1K2", 0D + 123))
				.containsEntry(Map.of("c", "c2"), Map.of("sumK1K2", 0D + 234));
	}

	@Test
	public void testFilterGroupBy() {
		ITabularView output =
				aqe.execute(AdhocQuery.builder().measure("sumK1K2").andFilter("c", "c1").groupByColumns("c").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 0D + 123));
	}
}
