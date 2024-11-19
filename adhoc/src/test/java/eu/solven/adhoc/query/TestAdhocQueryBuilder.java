package eu.solven.adhoc.query;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.api.v1.pojo.AndFilter;

public class TestAdhocQueryBuilder {
	@Test
	public void testGrandTotal() {
		AdhocQuery q = AdhocQuery.builder().build();

		Assertions.assertThat(q.getFilter().isMatchAll()).isTrue();
		Assertions.assertThat(q.getGroupBy().isGrandTotal()).isTrue();
		Assertions.assertThat(q.getMeasureRefs()).isEmpty();

		// Make sure the .toString returns actual values, and not the lambda toString
		Assertions.assertThat(q.toString()).doesNotContain("Lambda");
	}

	@Test
	public void testGrandTotal_filterAndEmpty() {
		AdhocQuery q = AdhocQuery.builder().andFilter(AndFilter.andAxisEqualsFilters(Map.of())).build();

		Assertions.assertThat(q.getFilter().isMatchAll()).isTrue();
		Assertions.assertThat(q.getGroupBy().isGrandTotal()).isTrue();
		Assertions.assertThat(q.getMeasureRefs()).isEmpty();
	}

	@Test
	public void testEquals() {
		AdhocQuery q1 = AdhocQuery.builder().build();
		AdhocQuery q2 = AdhocQuery.builder().build();

		Assertions.assertThat(q1).isEqualTo(q2);
	}

	@Test
	public void testAddGroupBy() {
		AdhocQuery q1 = AdhocQuery.builder().groupByColumns("a", "b").groupByColumns("c", "d").build();

		Assertions.assertThat(q1.getGroupBy().getGroupedByColumns()).contains("a", "b", "c", "d");
	}
}
