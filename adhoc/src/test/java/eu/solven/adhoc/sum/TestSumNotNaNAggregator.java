package eu.solven.adhoc.sum;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.aggregations.sum.SumNotNaNAggregator;

public class TestSumNotNaNAggregator {
	SumNotNaNAggregator a = new SumNotNaNAggregator();

	@Test
	public void testSum_objects() {
		Assertions.assertThat(a.aggregate(null, null)).isEqualTo(null);
		Assertions.assertThat(a.aggregate(null, 1.2D)).isEqualTo(1.2D);
		Assertions.assertThat(a.aggregate(1.2D, null)).isEqualTo(1.2D);
	}

	@Test
	public void testSum_doubles() {
		Assertions.assertThat(a.aggregateDoubles(Double.NaN, 1.2D)).isEqualTo(1.2D);
		Assertions.assertThat(a.aggregateDoubles(1.2D, Double.NaN)).isEqualTo(1.2D);
		Assertions.assertThat(a.aggregateDoubles(Double.NaN, Double.NaN)).isEqualTo(0D);
	}
}
