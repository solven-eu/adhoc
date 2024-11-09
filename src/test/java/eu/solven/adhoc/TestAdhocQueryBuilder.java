package eu.solven.adhoc;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.api.v1.pojo.AxesFilterAnd;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.AdhocQueryBuilder;

public class TestAdhocQueryBuilder {
	@Test
	public void testGrandTotal() {
		AdhocQuery q = AdhocQueryBuilder.grandTotal().build();

		Assertions.assertThat(q.getFilters().isMatchAll()).isTrue();
		Assertions.assertThat(q.getGroupBys()).isEmpty();
		Assertions.assertThat(q.getMeasures()).isEmpty();

		// Make sure the .toString returns actual values, and not the lambda toString
		Assertions.assertThat(q.toString()).doesNotContain("Lambda");
	}

	@Test
	public void testGrandTotal_filterAndEmpty() {
		AdhocQuery q = AdhocQueryBuilder.grandTotal().andFilter(AxesFilterAnd.andAxisEqualsFilters(Map.of())).build();

		Assertions.assertThat(q.getFilters().isMatchAll()).isTrue();
		Assertions.assertThat(q.getGroupBys()).isEmpty();
		Assertions.assertThat(q.getMeasures()).isEmpty();
	}

	@Test
	public void testEquals() {
		AdhocQuery q1 = AdhocQueryBuilder.grandTotal().build();
		AdhocQuery q2 = AdhocQueryBuilder.grandTotal().build();

		Assertions.assertThat(q1).isEqualTo(q2);
	}
}
