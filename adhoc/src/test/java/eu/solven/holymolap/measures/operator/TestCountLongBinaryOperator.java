package eu.solven.holymolap.measures.operator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCountLongBinaryOperator {
	final CountBinaryOperator operator = new CountBinaryOperator();

	@Test
	public void testSimpleSum() {
		Assertions.assertThat(operator.applyAsLong(123, 456)).isEqualTo(123 + 456);
		Assertions.assertThat(operator.apply((Number) 123D, (Number) 456D)).isEqualTo(123L + 456L);
	}
}