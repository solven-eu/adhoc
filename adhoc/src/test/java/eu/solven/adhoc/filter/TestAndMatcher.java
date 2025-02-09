package eu.solven.adhoc.filter;

import eu.solven.adhoc.query.filter.value.AndMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAndMatcher {
    @Test
    public void testAndInEq() {
        AndMatcher a_and_aandb = AndMatcher.builder().operand(EqualsMatcher.isEqualTo("a")).operand(InMatcher.isIn("a", "b")).build();

        Assertions.assertThat(a_and_aandb).isEqualTo(EqualsMatcher.isEqualTo("a"));
    }
}
