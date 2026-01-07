package eu.solven.adhoc.dictionary.page;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLongArrayColumn {
	@Test
	public void testFromArray_empty() {
		IReadableColumn c = LongArrayColumn.builder().asArray(new long[0]).build();
		Assertions.assertThatThrownBy(() -> c.readValue(0)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
	}

	@Test
	public void testFromArray_1() {
		IReadableColumn c = LongArrayColumn.builder().asArray(new long[] { 123 }).build();
		Assertions.assertThat(c.readValue(0)).isEqualTo(123L);
	}

	@Test
	public void testFromArray_2() {
		IReadableColumn c = LongArrayColumn.builder().asArray(new long[] { 123, 234 }).build();
		Assertions.assertThat(c.readValue(0)).isEqualTo(123L);
		Assertions.assertThat(c.readValue(1)).isEqualTo(234L);
	}

}
