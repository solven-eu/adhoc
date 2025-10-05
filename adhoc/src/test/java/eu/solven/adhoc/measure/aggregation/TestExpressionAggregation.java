package eu.solven.adhoc.measure.aggregation;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.sum.ExpressionAggregation;

public class TestExpressionAggregation {
	ExpressionAggregation agg = new ExpressionAggregation();

	@Test
	public void testAggregtion() {
		Assertions.assertThat(agg.aggregate((Object) null, null)).isNull();

		Assertions.assertThat(agg.aggregate("left", null)).isEqualTo("left");
		Assertions.assertThat(agg.aggregate(null, "right")).isEqualTo("right");

		Assertions.assertThatThrownBy(() -> agg.aggregate("left", "right"))
				.isInstanceOf(UnsupportedOperationException.class);
	}
}
