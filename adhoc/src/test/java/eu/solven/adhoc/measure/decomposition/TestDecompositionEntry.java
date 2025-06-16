package eu.solven.adhoc.measure.decomposition;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.data.cell.IValueProvider;

public class TestDecompositionEntry {
	@Test
	public void testToString() {
		Assertions.assertThat(IDecompositionEntry.of(Map.of("k", "v"), IValueProvider.setValue(123)))
				.hasToString("slice={k=v} value=123");
	}
}
