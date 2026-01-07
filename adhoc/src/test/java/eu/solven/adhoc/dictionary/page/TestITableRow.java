package eu.solven.adhoc.dictionary.page;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.util.immutable.UnsupportedAsImmutableException;

public class TestITableRow {
	@Test
	public void testEmpty() {
		ITableRowWrite empty = ITableRowWrite.empty();
		Assertions.assertThat(empty.size()).isEqualTo(0);
		Assertions.assertThatThrownBy(() -> empty.add("k", 0L)).isInstanceOf(UnsupportedAsImmutableException.class);
		Assertions.assertThat(empty.freeze()).isEqualTo(ITableRowRead.empty());
	}
}
