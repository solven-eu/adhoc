package eu.solven.adhoc.filter;

import eu.solven.adhoc.api.v1.pojo.ColumnFilter;
import eu.solven.adhoc.api.v1.pojo.LikeMatcher;
import eu.solven.adhoc.execute.FilterHelpers;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

public class TestFilterHelpers {
    @Test
    public void testIn() {
        ColumnFilter inV1OrV2 = ColumnFilter.builder().column("c").matching(Set.of("v1", "v2")).build();
        Assertions.assertThat(FilterHelpers.match(inV1OrV2, Map.of())).isFalse();
        Assertions.assertThat(FilterHelpers.match(inV1OrV2, Map.of("c", "v3"))).isFalse();

        Assertions.assertThat(FilterHelpers.match(inV1OrV2, Map.of("c", "v1"))).isTrue();
        Assertions.assertThat(FilterHelpers.match(inV1OrV2, Map.of("c", "v2"))).isTrue();
    }

    @Test
    public void testLike() {
        ColumnFilter startsWithV1 = ColumnFilter.builder().column("c").valueMatcher(LikeMatcher.builder().like("v1%") .build()).build();

        Assertions.assertThat(FilterHelpers.match(startsWithV1, Map.of())).isFalse();
        Assertions.assertThat(FilterHelpers.match(startsWithV1, Map.of("c", "v3"))).isFalse();

        Assertions.assertThat(FilterHelpers.match(startsWithV1, Map.of("c", "v1"))).isTrue();
        Assertions.assertThat(FilterHelpers.match(startsWithV1, Map.of("c", "v2"))).isFalse();

        Assertions.assertThat(FilterHelpers.match(startsWithV1, Map.of("c", "p_v1"))).isFalse();
        Assertions.assertThat(FilterHelpers.match(startsWithV1, Map.of("c", "v1_s"))).isTrue();
    }
}
