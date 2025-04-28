package eu.solven.adhoc.filter;

import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.NotFilter;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestNotFilter {
    @Test
    public void testNot() {
        Assertions.assertThat(NotFilter.not(IAdhocFilter.MATCH_ALL)).isEqualTo(IAdhocFilter.MATCH_NONE);
        Assertions.assertThat(NotFilter.not(IAdhocFilter.MATCH_NONE)).isEqualTo(IAdhocFilter.MATCH_ALL);
    }

}
