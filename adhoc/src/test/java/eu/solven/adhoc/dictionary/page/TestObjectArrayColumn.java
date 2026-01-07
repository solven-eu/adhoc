package eu.solven.adhoc.dictionary.page;

import java.util.List;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestObjectArrayColumn {
	@Test
	public void testFromArray_empty() {
		IReadableColumn c = ObjectArrayColumn.builder().asArray(List.of()).build();
		Assertions.assertThatThrownBy(() -> c.readValue(0)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
	}

	@Test
	public void testFromArray_1() {
		IReadableColumn c = ObjectArrayColumn.builder().asArray(List.of("a")).build();
		Assertions.assertThat(c.readValue(0)).isEqualTo("a");
	}

	@Test
	public void testFromArray_2_mixedTypes() {
		IReadableColumn c = ObjectArrayColumn.builder().asArray(List.of("a", 123L)).build();
		Assertions.assertThat(c.readValue(0)).isEqualTo("a");
		Assertions.assertThat(c.readValue(1)).isEqualTo(123L);
	}

	@Test
	public void testFromArray_freeze_toDic() {
		List<Object> list = IntStream.range(0, 32).<Object>mapToObj(i -> i % 2 == 0 ? "a" : "b").toList();

		IReadableColumn c = ObjectArrayColumn.builder().asArray(list).build().freeze();
		Assertions.assertThat(c).isInstanceOf(DictionarizedObjectColumn.class);
		Assertions.assertThat(c.readValue(0)).isEqualTo("a");
		Assertions.assertThat(c.readValue(1)).isEqualTo("b");
	}

	@Test
	public void testFromArray_freeze_toLong() {
		List<Object> list = IntStream.range(0, 32).<Object>mapToObj(i -> (long) i).toList();

		IReadableColumn c = ObjectArrayColumn.builder().asArray(list).build().freeze();
		Assertions.assertThat(c).isInstanceOf(LongArrayColumn.class);
		Assertions.assertThat(c.readValue(0)).isEqualTo(0L);
		Assertions.assertThat(c.readValue(1)).isEqualTo(1L);
	}

}
