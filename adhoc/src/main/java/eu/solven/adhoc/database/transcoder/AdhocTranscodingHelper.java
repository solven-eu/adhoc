/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.database.transcoder;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

/**
 * Helps transcoding from one column-model to another. Typically as measures may refer to a given set of columns, while
 * underlying database may have different names for these columns.
 */
@Slf4j
public class AdhocTranscodingHelper {
	protected AdhocTranscodingHelper() {
		// hidden
	}

	public static Map<String, ?> transcode(IAdhocDatabaseReverseTranscoder reverseTranscoder,
			Map<String, ?> underlyingMap) {
		Map<String, Object> transcoded = new HashMap<>();

		underlyingMap.forEach((underlyingKey, v) -> {
			Set<String> queriedKeys = reverseTranscoder.queried(underlyingKey);

			if (queriedKeys.isEmpty()) {
				// This output column was not requested, but it has been received. The DB returns unexpected columns?
				String queriedKey = underlyingKey;
				insertTranscoded(v, queriedKey, transcoded);
			} else {
				queriedKeys.forEach(queriedKey -> {
					insertTranscoded(v, queriedKey, transcoded);
				});
			}
		});

		return transcoded;
	}

	private static void insertTranscoded(Object v, String queriedKey, Map<String, Object> transcoded) {
		Object replaced = transcoded.put(queriedKey, v);

		if (replaced != null && !replaced.equals(v)) {
			// BEWARE Should we drop a static method as this may be customized?
			log.warn(
					"Transcoding led to an ambiguity as multiple underlyingKeys has queriedKey={} mapping to values {} and {}",
					queriedKey,
					replaced,
					v);
		}
	}
}
