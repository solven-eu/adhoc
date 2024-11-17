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
		List<ColumnFilter> filters = IntStream.range(0, 128)
				.mapToObj(i -> ColumnFilter.builder().column("k").filtered(i).build())
				.collect(Collectors.toList());

		Assertions.assertThat(AndFilter.and(filters).toString())
				.contains("filtered=0", "filtered=4")
				.doesNotContain("filtered=5")
				.hasSizeLessThan(256);
	}
}
