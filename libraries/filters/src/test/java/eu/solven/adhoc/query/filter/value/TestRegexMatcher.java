package eu.solven.adhoc.query.filter.value;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRegexMatcher {
	@Test
	public void testNominal() {
		IValueMatcher matcher = RegexMatcher.matching("a\\d+");

		Assertions.assertThat(matcher.match("a")).isFalse();
		Assertions.assertThat(matcher.match("a123")).isTrue();
	}
}
