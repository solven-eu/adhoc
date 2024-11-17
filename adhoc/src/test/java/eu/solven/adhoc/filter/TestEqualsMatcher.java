package eu.solven.adhoc.filter;

import java.util.HashMap;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.api.v1.pojo.ColumnFilter;
import eu.solven.adhoc.api.v1.pojo.NotFilter;
import eu.solven.adhoc.execute.FilterHelpers;

public class TestEqualsMatcher {
	@Test
	public void testMatchNull() {
		ColumnFilter kIsNull = ColumnFilter.builder().column("k").matchNull().build();

		Assertions.assertThat(kIsNull.getFiltered()).isNull();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of())).isTrue();
		Map<String, Object> explicitNull = new HashMap<>();
		explicitNull.put("k", null);
		Assertions.assertThat(FilterHelpers.match(kIsNull, explicitNull)).isTrue();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v"))).isFalse();
	}

	@Test
	public void testMatchNotNull() {
		ColumnFilter kIsNull = ColumnFilter.builder().column("k").matchNull().build();
		NotFilter kIsNotNull = NotFilter.builder().negated(kIsNull).build();

		Assertions.assertThat(FilterHelpers.match(kIsNotNull, Map.of())).isFalse();
		Map<String, Object> explicitNull = new HashMap<>();
		explicitNull.put("k", null);
		Assertions.assertThat(FilterHelpers.match(kIsNotNull, explicitNull)).isFalse();

		Assertions.assertThat(FilterHelpers.match(kIsNotNull, Map.of("k", "v"))).isTrue();
	}
}
