package eu.solven.adhoc.filter;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.pojo.AndFilter;
import eu.solven.adhoc.api.v1.pojo.ColumnFilter;
import eu.solven.adhoc.api.v1.pojo.IValueMatcher;
import eu.solven.adhoc.api.v1.pojo.LikeMatcher;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestLikeFilter {
	@Test
	public void testJustText() {
		IValueMatcher matcher = LikeMatcher.builder().like("a").build();

		Assertions.assertThat(matcher.match("a")).isTrue();
		Assertions.assertThat(matcher.match("A")).isTrue();
		Assertions.assertThat(matcher.match("a1")).isFalse();
		Assertions.assertThat(matcher.match("1a")).isFalse();
		Assertions.assertThat(matcher.match("1a1a1")).isFalse();

		Assertions.assertThat(matcher.match("b")).isFalse();
	}

	@Test
	public void testStartsWith() {
		IValueMatcher matcher = LikeMatcher.builder().like("a%").build();

		Assertions.assertThat(matcher.match("a")).isTrue();
		Assertions.assertThat(matcher.match("A")).isTrue();
		Assertions.assertThat(matcher.match("a1")).isTrue();
		Assertions.assertThat(matcher.match("1a")).isFalse();

		Assertions.assertThat(matcher.match("1a1a1")).isFalse();
		Assertions.assertThat(matcher.match("1a1b1")).isFalse();
		Assertions.assertThat(matcher.match("1a1a1b1")).isFalse();

		Assertions.assertThat(matcher.match("b")).isFalse();
	}


	@Test
	public void testContains() {
		IValueMatcher matcher = LikeMatcher.builder().like("%a%").build();

		Assertions.assertThat(matcher.match("a")).isTrue();
		Assertions.assertThat(matcher.match("A")).isTrue();
		Assertions.assertThat(matcher.match("a1")).isTrue();
		Assertions.assertThat(matcher.match("1a")).isTrue();

		Assertions.assertThat(matcher.match("1a1a1")).isTrue();
		Assertions.assertThat(matcher.match("1a1b1")).isTrue();
		Assertions.assertThat(matcher.match("1a1a1b1")).isTrue();

		Assertions.assertThat(matcher.match("b")).isFalse();
	}

	@Test
	public void testContainsTwo() {
		IValueMatcher matcher = LikeMatcher.builder().like("%a%b%").build();

		Assertions.assertThat(matcher.match("a")).isFalse();
		Assertions.assertThat(matcher.match("A")).isFalse();
		Assertions.assertThat(matcher.match("a1")).isFalse();
		Assertions.assertThat(matcher.match("1a")).isFalse();

		Assertions.assertThat(matcher.match("1a1a1")).isFalse();
		Assertions.assertThat(matcher.match("1a1b1")).isTrue();
		Assertions.assertThat(matcher.match("1a1a1b1")).isTrue();

		Assertions.assertThat(matcher.match("b")).isFalse();
	}

	@Test
	public void testTwiceThenOther() {
		IValueMatcher matcher = LikeMatcher.builder().like("%a%a%b%").build();

		Assertions.assertThat(matcher.match("a")).isFalse();
		Assertions.assertThat(matcher.match("A")).isFalse();
		Assertions.assertThat(matcher.match("a1")).isFalse();
		Assertions.assertThat(matcher.match("1a")).isFalse();

		Assertions.assertThat(matcher.match("1a1a1")).isFalse();
		Assertions.assertThat(matcher.match("1a1b1")).isFalse();
		Assertions.assertThat(matcher.match("1a1a1b1")).isTrue();

		Assertions.assertThat(matcher.match("b")).isFalse();
	}
}
