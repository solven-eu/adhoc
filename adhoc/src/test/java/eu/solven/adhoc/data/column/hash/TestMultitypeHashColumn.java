package eu.solven.adhoc.data.column.hash;

import java.util.List;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMultitypeHashColumn {
	MultitypeHashColumn<String> column = MultitypeHashColumn.<String>builder().build();

	// TODO Improve this test to actually for footprint
	// e.g. with PepperFootprintHelper
	@Test
	public void testCompact() {
		int size = 128;
		List<?> asList = IntStream.range(0, size).mapToObj(i -> MultitypeHashColumn.builder().build()).peek(c -> {
			c.append("k").onLong(1);
		}).peek(c -> {
			c.compact();
		}).toList();

		Assertions.assertThat(asList).hasSize(size);
	}
}
