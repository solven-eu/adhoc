package eu.solven.adhoc.encoding.column.freezer;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.encoding.bytes.Utf8ByteSlice;
import eu.solven.adhoc.encoding.column.IReadableColumn;
import eu.solven.adhoc.encoding.column.ObjectArrayColumn;

public class TestUtf8ToStringFreezer {
	Utf8ToStringFreezer freezer = new Utf8ToStringFreezer();

	@Test
	public void testFreeze_empty() {
		ObjectArrayColumn column = ObjectArrayColumn.builder().build();
		Optional<IReadableColumn> optFrozen = freezer.freeze(column, new HashMap<>());
		Assertions.assertThat(optFrozen).isEmpty();
	}

	@Test
	public void testFreeze_mixed() {
		ObjectArrayColumn column = ObjectArrayColumn.builder().build();

		column.append(Utf8ByteSlice.fromString("foo"));
		column.append(123L);
		column.append(Utf8ByteSlice.fromString("bar"));
		column.append(234L);

		Optional<IReadableColumn> optFrozen = freezer.freeze(column, new HashMap<>());
		Assertions.assertThat(optFrozen).isPresent().get().isInstanceOfSatisfying(ObjectArrayColumn.class, frozen -> {
			Assertions.assertThat((List) frozen.getAsArray()).containsExactly("foo", 123L, "bar", 234L);
		});
	}
}
