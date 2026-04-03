package eu.solven.adhoc.dataframe.aggregating;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;

public class TestPartitionedMultitypeMergeableGrid {
	AggregatingColumns<String> p1 = AggregatingColumns.<String>builder().build();
	AggregatingColumns<String> p2 = AggregatingColumns.<String>builder().build();

	PartitionedMultitypeMergeableGrid<String, Integer> partitioned =
			PartitionedMultitypeMergeableGrid.<String, Integer>builder().partition(p1).partition(p2).build();

	Aggregator sum = Aggregator.sum("k1");

	@Test
	public void testEmpty() {
		Assertions.assertThat(partitioned.getAggregators()).isEmpty();
		Assertions.assertThat(partitioned.getNbPartitions()).isEqualTo(2);

		Assertions.assertThat(partitioned)
				.hasToString(
						"PartitionedMultitypeMergeableGrid{nbPartitions=2, partition:0=AggregatingColumns{#slices=0, aggregators=0}, partition:1=AggregatingColumns{#slices=0, aggregators=0}}");
	}

	@Test
	public void testInsertSingleRow() {
		partitioned.openSlice("a1").contribute(sum).onLong(123);

		Assertions.assertThat(partitioned.getAggregators()).containsExactly("k1");

		Assertions.assertThat(partitioned)
				.hasToString(
						"PartitionedMultitypeMergeableGrid{nbPartitions=2, partition:0=AggregatingColumns{#slices=1, aggregators=1, a1={k1=123}}, partition:1=AggregatingColumns{#slices=0, aggregators=0}}");

		IMultitypeColumnFastGet<String> column =
				partitioned.closeColumn(CubeQueryStep.builder().measure("k1").build(), sum);

		Assertions.assertThat(column)
				.hasToString(
						"PartitionedColumn{nbPartitions=2, partition:0=a1=123, partition:1=MultitypeHashColumn{}}");
	}
}
