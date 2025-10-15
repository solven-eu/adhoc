package eu.solven.adhoc.eventbus;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAdhocLogEvent {
	@Test
	public void testTemplate() {
		Assertions.assertThat(
				AdhocLogEvent.builder().source("someSource").messageT("a - %s - {}", "b", "c").build().getMessage())
				.isEqualTo("a - b - c");
	}
}
