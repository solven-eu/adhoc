package eu.solven.adhoc.filter;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.pojo.AndFilter;
import eu.solven.adhoc.api.v1.pojo.ColumnFilter;

public class TestAndFilter {
	// A short toString not to prevail is composition .toString
	@Test
	public void toString_grandTotal() {
		Assertions.assertThat(IAdhocFilter.MATCH_ALL.toString()).isEqualTo("matchAll");
	}

	@Test
	public void toString_huge() {
		List<ColumnFilter> filters = IntStream.range(0, 256)
				.mapToObj(i -> ColumnFilter.builder().column("k").matching(i).build())
				.collect(Collectors.toList());

		Assertions.assertThat(AndFilter.and(filters).toString())
				.contains("valueMatcher=EqualsMatcher(operand=0)", "valueMatcher=EqualsMatcher(operand=0)")
				.doesNotContain("7")
				.hasSizeLessThan(512);
	}

	@Test
	public void testAndFilters_twoGrandTotal() {
		IAdhocFilter filterAllAndA = AndFilter.and(IAdhocFilter.MATCH_ALL, IAdhocFilter.MATCH_ALL);

		Assertions.assertThat(filterAllAndA).isEqualTo(IAdhocFilter.MATCH_ALL);
	}

	@Test
	public void testAndFilters_oneGrandTotal() {
		IAdhocFilter filterAllAndA = AndFilter.and(IAdhocFilter.MATCH_ALL, ColumnFilter.isEqualTo("a", "a1"));

		Assertions.assertThat(filterAllAndA).isEqualTo(ColumnFilter.isEqualTo("a", "a1"));
	}

	@Test
	public void testAndFilters_oneGrandTotal_TwoCustom() {
		IAdhocFilter filterAllAndA = AndFilter
				.and(IAdhocFilter.MATCH_ALL, ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("a", "a2"));

		Assertions.assertThat(filterAllAndA).isInstanceOfSatisfying(AndFilter.class, andF -> {
			Assertions.assertThat(andF.getAnd())
					.hasSize(2)
					.contains(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("a", "a2"));
		});
	}
}
