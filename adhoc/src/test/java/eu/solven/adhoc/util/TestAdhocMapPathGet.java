package eu.solven.adhoc.util;

import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAdhocMapPathGet {

	@Test
	public void testFaultInKey_type() {
		Map<String, Object> input = Map.of("underlyings", List.of("k1", "k2"));

		Assertions.assertThatThrownBy(() -> AdhocMapPathGet.getRequiredList(input, "undelryings"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Did you mean `underlyings` instead of `undelryings`");
	}

	@Test
	public void testFaultInKey_missing() {
		Map<String, Object> input = Map.of("type", "combinator");

		Assertions.assertThatThrownBy(() -> AdhocMapPathGet.getRequiredList(input, "underlyings"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageNotContaining("instead of");
	}
}
