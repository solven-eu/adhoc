package eu.solven.adhoc.database;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class AdhocTranscodingHelper {
    protected AdhocTranscodingHelper() {
        // hidden
    }

    public static Map<String, ?> transcode(IAdhocDatabaseTranscoder transcoder, Map<String, ?> underlyingMap) {
        Map<String, Object> transcoded = new HashMap<>();

        underlyingMap.forEach((underlyingKey, v) -> {
            String queriedKey = transcoder.queried(underlyingKey);
            Object replaced = transcoded.put(queriedKey, v);

            if (replaced != null && !replaced.equals(v)) {
                log.warn("Transcoding led to an ambiguity as multiple underlyingKeys has queriedKey={} mapping to values {} and {}", queriedKey, replaced, v);
            }
        });

        return transcoded;
    }
}
