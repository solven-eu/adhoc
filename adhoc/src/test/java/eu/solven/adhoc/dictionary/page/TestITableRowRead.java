package eu.solven.adhoc.dictionary.page;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestITableRowRead {
	@Test
	public void testEmpty() {
		ITableRowRead empty = ITableRowRead.empty();
		Assertions.assertThat(empty.size()).isEqualTo(0);
		Assertions.assertThat(empty.readValue(0)).isNull();
	}
}
