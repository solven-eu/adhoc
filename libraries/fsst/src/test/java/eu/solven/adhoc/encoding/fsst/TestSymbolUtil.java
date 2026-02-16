package eu.solven.adhoc.encoding.fsst;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSymbolUtil {
	@Test
	public void testEmpty() {
		Assertions.assertThatThrownBy(() -> SymbolUtil.fsstUnalignedLoad(new byte[] { 1, 2, 3 }, 3))
				.isInstanceOf(IllegalArgumentException.class);
		Assertions.assertThatThrownBy(() -> SymbolUtil.fsstUnalignedLoad(new byte[] { 1, 2, 3 }, 5))
				.isInstanceOf(IllegalArgumentException.class);
	}
}
