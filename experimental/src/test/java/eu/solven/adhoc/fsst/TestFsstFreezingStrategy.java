package eu.solven.adhoc.fsst;

import java.util.ArrayList;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.compression.column.IAppendableColumn;
import eu.solven.adhoc.compression.column.ObjectArrayColumn;
import eu.solven.adhoc.compression.page.IReadableColumn;

public class TestFsstFreezingStrategy {
	@Test
	public void testStrings() {
		FsstFreezingStrategy s = new FsstFreezingStrategy();

		IAppendableColumn column = ObjectArrayColumn.builder().freezer(s).asArray(new ArrayList<>()).build();

		column.append("a");
		column.append("b");

		IReadableColumn frozen = column.freeze();

		Assertions.assertThat(frozen).isInstanceOf(FsstReadableColumn.class);
		Assertions.assertThat(frozen.readValue(0)).isEqualTo("a");
		Assertions.assertThat(frozen.readValue(1)).isEqualTo("b");
	}

	@Test
	public void testStrings_withNull() {
		FsstFreezingStrategy s = new FsstFreezingStrategy();

		IAppendableColumn column = ObjectArrayColumn.builder().freezer(s).asArray(new ArrayList<>()).build();

		column.append("a");
		column.append(null);
		column.append("b");

		IReadableColumn frozen = column.freeze();

		Assertions.assertThat(frozen).isInstanceOf(FsstReadableColumn.class);
		Assertions.assertThat(frozen.readValue(0)).isEqualTo("a");
		Assertions.assertThat(frozen.readValue(1)).isEqualTo(null);
		Assertions.assertThat(frozen.readValue(2)).isEqualTo("b");
	}
}
