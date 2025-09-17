package eu.solven.adhoc.query.filter;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFilterEqualsHelpers {
	@Test
	public void testEquivalent_inEquivalentOr() {
		ISliceFilter in = ColumnFilter.isIn("c", "c1", "c2");
		ISliceFilter or =
				OrFilter.builder().or(ColumnFilter.isEqualTo("c", "c1")).or(ColumnFilter.isEqualTo("c", "c2")).build();

		Assertions.assertThat(in).isNotEqualTo(or);
		Assertions.assertThat(FilterEquivalencyHelpers.areEquivalent(in, or)).isTrue();
	}

	@Test
	public void testEquivalent_andOrEquivalentListedOr() {
		ISliceFilter inA = ColumnFilter.isIn("a", "a1", "a2");
		ISliceFilter inB = ColumnFilter.isIn("b", "b1", "b2");
		ISliceFilter and = AndFilter.builder().and(inA).and(inB).build();
		ISliceFilter or = OrFilter.builder()
				.or(AndFilter.and(Map.of("a", "a1", "b", "b1")))
				.or(AndFilter.and(Map.of("a", "a1", "b", "b2")))
				.or(AndFilter.and(Map.of("a", "a2", "b", "b1")))
				.or(AndFilter.and(Map.of("a", "a2", "b", "b2")))
				.build();

		Assertions.assertThat(and).isNotEqualTo(or);
		Assertions.assertThat(FilterEquivalencyHelpers.areEquivalent(and, or)).isTrue();
	}
}
