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
package eu.solven.adhoc.table.transcoder;

import java.util.*;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.NonNull;

/**
 * A decorating {@link ITableTranscoder} , which applies the transcoding logic recursively
 */
@Builder
public class RecursiveTranscoder implements ITableTranscoder {
	/**
	 * The maximum depth of recursivity
	 */
	public static final int LIMIT = 1024;

	@NonNull
	final ITableTranscoder transcoder;

	public static ITableTranscoder wrap(ITableTranscoder transcoder) {
		return RecursiveTranscoder.builder().transcoder(transcoder).build();
	}

	@Override
	public String underlying(String queried) {
		Set<String> querieds = Collections.emptySet();

		boolean first = true;
		String nextQueried = queried;
		do {
			String nextUnderlying = transcoder.underlying(nextQueried);

			if (nextUnderlying == null) {
				// Not transcoded
				if (first) {
					return null;
				} else {
					return nextQueried;
				}
			} else if (nextUnderlying.equals(nextQueried)) {
				// Transcoded to itself
				return nextQueried;
			}

			if (first) {
				// Linkde for ordering in case of issue
				querieds = new LinkedHashSet<>();
			} else if (querieds.size() >= LIMIT) {
				throw new IllegalStateException(
						"Transcoding too-deep from `%s` and through: %s".formatted(queried, querieds));
			}
			if (!querieds.add(nextQueried)) {
				// `queried` has already been queried: this is a cycle
				List<String> pathWithCycle = Stream.concat(querieds.stream(), Stream.of(nextQueried)).toList();
				throw new IllegalStateException(
						"Transcoding cycle from `%s` and through: %s".formatted(queried, pathWithCycle));
			}

			first = false;
			nextQueried = nextUnderlying;
		} while (true);
	}
}
