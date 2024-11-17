package eu.solven.adhoc.measure;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.transformers.Aggregator;

public class TestAggregator {
	@Test
	public void testWithoutColumnName() {
		Aggregator aggregator = Aggregator.builder().name("someName").build();

		Assertions.assertThat(aggregator.getName()).isEqualTo("someName");
		Assertions.assertThat(aggregator.getColumnName()).isEqualTo("someName");
		Assertions.assertThat(aggregator.getAggregationKey()).isEqualTo(SumAggregator.KEY);
	}

	@Test
	public void testWithColumnName() {
		Aggregator aggregator = Aggregator.builder().name("someName").columnName("otherColumnName").build();

		Assertions.assertThat(aggregator.getName()).isEqualTo("someName");
		Assertions.assertThat(aggregator.getColumnName()).isEqualTo("otherColumnName");
		Assertions.assertThat(aggregator.getAggregationKey()).isEqualTo(SumAggregator.KEY);
	}
}
