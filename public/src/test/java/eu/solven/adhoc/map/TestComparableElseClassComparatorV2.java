package eu.solven.adhoc.map;

import java.util.Comparator;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestComparableElseClassComparatorV2 {
	Comparator<Object> comparator = new ComparableElseClassComparatorV2();

	@Test
	public void testCompare_null() {
		Assertions.assertThat(comparator.compare(1, null)).isEqualTo(-1);
		Assertions.assertThat(comparator.compare(null, "foo")).isEqualTo(1);
	}

	@Test
	public void testCompare_sameClass() {
		Assertions.assertThat(comparator.compare(1, 2)).isEqualTo(-1);
		Assertions.assertThat(comparator.compare("foo", "bar")).isEqualTo(4);

		Assertions.assertThat(comparator.compare("foo", 123)).isEqualTo(10);
		Assertions.assertThat(comparator.compare(123, "foo")).isEqualTo(-10);
	}

	@Test
	public void testCompare_notComparable() {
		Assertions.assertThat(comparator.compare(List.of("foo"), List.of("bar"))).isEqualTo(4);
		Assertions.assertThat(comparator.compare(List.of("bar"), List.of("foo"))).isEqualTo(-4);
	}
}
