package eu.solven.adhoc.table.transcoder;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMapTableTranscoder {
    @Test
    public void testNotRecursive() {
        MapTableTranscoder transcoder = MapTableTranscoder.builder().queriedToUnderlying("k", "_k").queriedToUnderlying("_k", "__k").build();

        Assertions.assertThat(transcoder.underlying("a")).isEqualTo(null);
        Assertions.assertThat(transcoder.underlying("k")).isEqualTo("_k");
        Assertions.assertThat(transcoder.underlying("_k")).isEqualTo("__k");
        Assertions.assertThat(transcoder.underlying("__k")).isEqualTo(null);
    }
}
