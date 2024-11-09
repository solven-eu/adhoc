package eu.solven.holymolap.measures.operator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSumLongBinaryOperator {
	final SumLongBinaryOperator operator = new SumLongBinaryOperator();

	@Test
	public void testSimpleSum() {
		Assertions.assertThat(operator.applyAsLong(123, 456)).isEqualTo(123 + 456);
		Assertions.assertThat(operator.applyAsDouble(123D, 456D)).isEqualTo(123 + 456);
		Assertions.assertThat(operator.apply((Number) 123D, (Number) 456D)).isEqualTo(123D + 456D);
	}
}