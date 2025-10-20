package eu.solven.adhoc.query.filter.stripper;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.ISliceFilter;

public class TestFilterStripperFactory {
	@Test
	public void testMake() {
		FilterStripperFactory factory = FilterStripperFactory.builder().build();
		ISliceFilter filter = ColumnFilter.matchEq("c", "v");
		IFilterStripper stripper = factory.makeFilterStripper(filter);

		Assertions.assertThat(stripper).isInstanceOfSatisfying(FilterStripper.class, s -> {
			Assertions.assertThat(s.getWhere()).isEqualTo(filter);
		});
	}
}
