package eu.solven.adhoc.table.transcoder;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPrefixAliaser {
	PrefixAliaser aliaser = PrefixAliaser.builder().prefix("p_").build();

	@Test
	public void testAlias() {
		Assertions.assertThat(aliaser.underlying("k")).isEqualTo("p_k");
		Assertions.assertThat(aliaser.underlying("p_k")).isEqualTo("p_p_k");

		Assertions.assertThat(aliaser.queried("p_k")).containsExactly("k");

		Assertions.assertThatThrownBy(() -> aliaser.queried("k")).isInstanceOf(IllegalArgumentException.class);

		Assertions.assertThat(aliaser.estimateQueriedSize(Set.of("k"))).isEqualTo(1);
	}
}
