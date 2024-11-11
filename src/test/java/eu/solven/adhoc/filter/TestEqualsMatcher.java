package eu.solven.adhoc.filter;

import java.util.HashMap;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.api.v1.pojo.EqualsFilter;
import eu.solven.adhoc.api.v1.pojo.FilterHelpers;
import eu.solven.adhoc.api.v1.pojo.NotFilter;

public class TestEqualsMatcher {
	@Test
	public void testMatchNull() {
		EqualsFilter kIsNull = EqualsFilter.builder().axis("k").matchNull().build();

		Assertions.assertThat(kIsNull.getFiltered()).isNull();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of())).isTrue();
		Map<String, Object> explicitNull = new HashMap<>();
		explicitNull.put("k", null);
		Assertions.assertThat(FilterHelpers.match(kIsNull, explicitNull)).isTrue();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v"))).isFalse();
	}

	@Test
	public void testMatchNotNull() {
		EqualsFilter kIsNull = EqualsFilter.builder().axis("k").matchNull().build();
		NotFilter kIsNotNull = NotFilter.builder().negated(kIsNull).build();

		Assertions.assertThat(FilterHelpers.match(kIsNotNull, Map.of())).isFalse();
		Map<String, Object> explicitNull = new HashMap<>();
		explicitNull.put("k", null);
		Assertions.assertThat(FilterHelpers.match(kIsNotNull, explicitNull)).isFalse();

		Assertions.assertThat(FilterHelpers.match(kIsNotNull, Map.of("k", "v"))).isTrue();
	}
}
