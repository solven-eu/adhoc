package eu.solven.adhoc.query.cube;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.value.IValueMatcher;

public class TestCubeQuery {
	@Test
	public void testCubeQuery() {
		ISliceFilter filter =
				CubeQuery.builder().andFilter("a", "a1").andFilter("b", IValueMatcher.MATCH_ALL).build().getFilter();
		Assertions.assertThat(filter).hasToString("a==a1");
	}
}
