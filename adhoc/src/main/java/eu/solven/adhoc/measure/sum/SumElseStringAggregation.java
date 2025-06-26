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
package eu.solven.adhoc.measure.sum;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.IDoubleAggregation;
import eu.solven.adhoc.measure.aggregation.ILongAggregation;
import lombok.extern.slf4j.Slf4j;

/**
 * A `SUM` {@link IAggregation}. On any {@link Object} which is neither a {@link Long} or a {@link Double}, this will
 * consider the {@link Object#toString()}.
 * 
 * @author Benoit Lacelle
 */
// https://learn.microsoft.com/en-us/dax/sum-function-dax
@Slf4j
public class SumElseStringAggregation extends SumAggregation implements IDoubleAggregation, ILongAggregation {

	@Override
	protected Object wrapNotANumber(Object r) {
		if (r == null) {
			return r;
		} else {
			return r.toString();
		}
	}

	@SuppressWarnings("checkstyle:MagicNumber")
	@Override
	protected Object aggregateObjects(Object l, Object r) {
		// Fallback by concatenating Strings
		String concatenated = l.toString() + r.toString();

		if (concatenated.length() >= 16 * 1024) {
			// If this were a real use-case, we should probably rely on a dedicated optimized IAggregation
			throw new IllegalStateException("Aggregation led to a too-large (length=%s) String: %s"
					.formatted(concatenated.length(), concatenated));
		} else if (concatenated.length() >= 1024) {
			log.warn("Aggregation led to a large String: {}", concatenated);
		}

		return concatenated;
	}

}
