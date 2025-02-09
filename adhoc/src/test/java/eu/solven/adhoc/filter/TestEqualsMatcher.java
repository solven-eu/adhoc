package eu.solven.adhoc.filter;

import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class TestEqualsMatcher {
    @Test
    public void testSimple() {
        IValueMatcher equalsMatcher = EqualsMatcher.isEqualTo("a");

        Assertions.assertThat(equalsMatcher.match("a")).isEqualTo(true);
        Assertions.assertThat(equalsMatcher.match(Set.of("a"))).isEqualTo(false);

        Assertions.assertThat(equalsMatcher.match(null)).isEqualTo(false);
    }

    @Test
    public void testNull() {
        Assertions.assertThatThrownBy(() -> EqualsMatcher.isEqualTo(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
