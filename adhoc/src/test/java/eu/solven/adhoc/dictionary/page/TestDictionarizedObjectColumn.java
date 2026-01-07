package eu.solven.adhoc.dictionary.page;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDictionarizedObjectColumn {
	@Test
	public void testFromArray_empty() {
		IReadableColumn c = DictionarizedObjectColumn.fromArray(List.of());
		Assertions.assertThatThrownBy(() -> c.readValue(0)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
	}

	@Test
	public void testFromArray_1() {
		IReadableColumn c = DictionarizedObjectColumn.fromArray(List.of("a"));
		Assertions.assertThat(c.readValue(0)).isEqualTo("a");
	}

	@Test
	public void testFromArray_2() {
		IReadableColumn c = DictionarizedObjectColumn.fromArray(List.of(new String("a"), new String("a")));
		Assertions.assertThat(c.readValue(0)).isEqualTo("a");
		Assertions.assertThat(c.readValue(1)).isSameAs(c.readValue(0));
	}

	@Test
	public void testFromArray_mixed() {
		IReadableColumn c = DictionarizedObjectColumn.fromArray(List.of("a", "b", "a", "c", "b"));

		Assertions.assertThat(c.readValue(0)).isEqualTo("a");
		Assertions.assertThat(c.readValue(1)).isEqualTo("b");
		Assertions.assertThat(c.readValue(2)).isEqualTo("a");
		Assertions.assertThat(c.readValue(3)).isEqualTo("c");
		Assertions.assertThat(c.readValue(4)).isEqualTo("b");
	}
}
