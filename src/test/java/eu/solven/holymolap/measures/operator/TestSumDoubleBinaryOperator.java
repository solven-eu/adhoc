package eu.solven.holymolap.measures.operator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSumDoubleBinaryOperator {
	final SumDoubleBinaryOperator operator = new SumDoubleBinaryOperator();

	@Test
	public void testSimpleSum() {
		Assertions.assertThat(operator.applyAsDouble(123D, 456D)).isEqualTo(123D + 456D);
		Assertions.assertThat(operator.apply((Number) 123D, (Number) 456D)).isEqualTo(123D + 456D);
	}

	@Test
	public void testWithNaN() {
		Assertions.assertThat(operator.applyAsDouble(Double.NaN, 456D)).isNaN();
		Assertions.assertThat(operator.applyAsDouble(123D, Double.NaN)).isNaN();
		Assertions.assertThat(operator.applyAsDouble(Double.NaN, Double.NaN)).isNaN();
	}
}