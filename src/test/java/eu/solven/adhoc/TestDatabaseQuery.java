package eu.solven.adhoc;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.DatabaseQuery;

public class TestDatabaseQuery implements IAdhocTestConstants {
	@Test
	public void testGrandTotal() {
		DatabaseQuery q = DatabaseQuery.builder().aggregators(Set.of(k1Sum)).build();

		Assertions.assertThat(q.getFilters().isMatchAll()).isTrue();
		Assertions.assertThat(q.getGroupBys()).isEmpty();
		Assertions.assertThat(q.getAggregators()).hasSize(1).contains(k1Sum);

		// Make sure the .toString returns actual values, and not the lambda toString
		Assertions.assertThat(q.toString()).doesNotContain("Lambda");
	}
}
