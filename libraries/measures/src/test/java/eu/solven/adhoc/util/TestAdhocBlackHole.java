package eu.solven.adhoc.util;

import org.junit.jupiter.api.Test;

public class TestAdhocBlackHole {
	@Test
	public void testNominal() {
		AdhocBlackHole.getInstance().onLong(123);
		AdhocBlackHole.getInstance().onDouble(12.34);
		AdhocBlackHole.getInstance().onObject("foo");
		AdhocBlackHole.getInstance().post("foo");
	}
}
