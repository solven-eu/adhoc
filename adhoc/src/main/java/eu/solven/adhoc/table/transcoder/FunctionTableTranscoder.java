package eu.solven.adhoc.table.transcoder;

import lombok.Builder;

import java.util.function.Function;

/**
 * A {@link IAdhocTableTranscoder} over a {@link Function}.
 */
@Builder
public class FunctionTableTranscoder implements IAdhocTableTranscoder {
    final Function<String, String> queriedToUnderlying;

    @Override
    public String underlying(String queried) {
        return queriedToUnderlying.apply(queried);
    }
}
