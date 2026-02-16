package eu.solven.adhoc.map;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAdhocMapComparisonHelpers {
	@Test
	public void testCompareString() {
		Assertions.assertThat(AdhocMapComparisonHelpers.compareKey("a", "c")).isEqualTo(-2);
	}

	@Test
	public void testCompareList() {
		Assertions.assertThat(AdhocMapComparisonHelpers.compareValues(List.of("a"), List.of("c"))).isEqualTo(-2);
		Assertions.assertThat(AdhocMapComparisonHelpers.compareValues(List.of("b", "a"), List.of("b", "c")))
				.isEqualTo(-2);
		Assertions.assertThat(AdhocMapComparisonHelpers.compareValues(List.of("a", "b"), List.of("c", "b")))
				.isEqualTo(-2);
	}

	@Test
	public void testCompareIterators() {
		Assertions
				.assertThat(AdhocMapComparisonHelpers.compareValues2(List.of("a").iterator(), List.of("c").iterator()))
				.isEqualTo(-2);
		Assertions
				.assertThat(AdhocMapComparisonHelpers.compareValues2(List.of("b", "a").iterator(),
						List.of("b", "c").iterator()))
				.isEqualTo(-2);
		Assertions
				.assertThat(AdhocMapComparisonHelpers.compareValues2(List.of("a", "b").iterator(),
						List.of("c", "b").iterator()))
				.isEqualTo(-2);
	}
}
