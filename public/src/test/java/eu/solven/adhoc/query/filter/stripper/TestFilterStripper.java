package eu.solven.adhoc.query.filter.stripper;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;

public class TestFilterStripper {
	@Test
	public void testSharedCache() {
		FilterStripper stripper = FilterStripper.builder().where(AndFilter.and(Map.of("c", "c1", "d", "d2"))).build();
		Assertions.assertThat(stripper.filterToStripper.asMap()).isEmpty();

		Assertions.assertThat(stripper.isStricterThan(ColumnFilter.matchEq("c", "c1"))).isTrue();
		Assertions.assertThat(stripper.filterToStripper.asMap()).hasSize(1);

		FilterStripper relatedStripper = stripper.withWhere(ColumnFilter.matchEq("e", "e3"));
		// Ensure the cache unrelated to current WHERE is shared
		Assertions.assertThat(relatedStripper.filterToStripper.asMap()).hasSize(1);
		// Ensure the cache related to current WHERE is not-shared
		Assertions.assertThat(relatedStripper.knownAsStricter.asMap()).isEmpty();
	}
}
