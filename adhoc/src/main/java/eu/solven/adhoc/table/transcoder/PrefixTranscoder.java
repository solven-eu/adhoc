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

import java.util.Set;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;

/**
 * A transcoder useful when it is known that all columns has a redundant prefix (e.g. from SQL schema).
 * 
 * @author Benoit Lacelle
 *
 */
@Builder
public class PrefixTranscoder implements ITableTranscoder, ITableReverseTranscoder {
	// If empty, it is like the IdentityTranscoder
	@NonNull
	@Default
	String prefix = "";

	@Override
	public String underlying(String queried) {
		return prefix + queried;
	}

	@Override
	public Set<String> queried(String underlying) {
		if (underlying.startsWith(prefix)) {
			String queried = underlying.substring(prefix.length());
			return Set.of(queried);
		} else {
			throw new IllegalArgumentException(
					"We received a column not prefixed by %s: %s".formatted(prefix, underlying));
		}
	}

	@Override
	public int estimateQueriedSize(Set<String> underlyingKeys) {
		return underlyingKeys.size();
	}
}
