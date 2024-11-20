package eu.solven.adhoc.filter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.api.v1.pojo.ColumnFilter;
import eu.solven.adhoc.api.v1.pojo.NotFilter;
import eu.solven.adhoc.execute.FilterHelpers;

public class TestColumnFilter {
	@Test
	public void testMatchNull() {
		ColumnFilter kIsNull = ColumnFilter.builder().column("k").matchNull().build();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of())).isTrue();
		Map<String, Object> explicitNull = new HashMap<>();
		explicitNull.put("k", null);
		Assertions.assertThat(FilterHelpers.match(kIsNull, explicitNull)).isTrue();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v"))).isFalse();
		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v2"))).isFalse();
	}

	@Test
	public void testMatchNullButNotMissing() {
		ColumnFilter kIsNull = ColumnFilter.builder().column("k").matchNull().nullIfAbsent(false).build();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of())).isFalse();
		Map<String, Object> explicitNull = new HashMap<>();
		explicitNull.put("k", null);
		Assertions.assertThat(FilterHelpers.match(kIsNull, explicitNull)).isTrue();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v"))).isFalse();
		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v2"))).isFalse();
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
		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v2"))).isFalse();
	}

	@Test
	public void testJavaUtilSetOf() {
		ColumnFilter kIsNull = ColumnFilter.builder().column("k").matching(Set.of("v")).build();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of())).isFalse();
		Map<String, Object> explicitNull = new HashMap<>();
		explicitNull.put("k", null);
		Assertions.assertThat(FilterHelpers.match(kIsNull, explicitNull)).isFalse();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v"))).isTrue();
		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v2"))).isFalse();
	}


	@Test
	public void testCollectionsWithNull() {
		ColumnFilter kIsNull = ColumnFilter.builder().column("k").matching(Arrays.asList(null, "v")).build();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of())).isTrue();
		Map<String, Object> explicitNull = new HashMap<>();
		explicitNull.put("k", null);
		Assertions.assertThat(FilterHelpers.match(kIsNull, explicitNull)).isTrue();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v"))).isTrue();
		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v2"))).isFalse();
	}

	@Test
	public void testIsDistinctFrom() {
		ColumnFilter kIsNull = ColumnFilter.isDistinctFrom("k", "v");

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of())).isTrue();
		Map<String, Object> explicitNull = new HashMap<>();
		explicitNull.put("k", null);
		Assertions.assertThat(FilterHelpers.match(kIsNull, explicitNull)).isTrue();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v"))).isFalse();
		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v2"))).isTrue();
	}
}
