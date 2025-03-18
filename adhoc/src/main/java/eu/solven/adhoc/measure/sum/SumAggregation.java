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

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.ICharSequenceAggregation;
import eu.solven.adhoc.measure.aggregation.IDoubleAggregation;
import eu.solven.adhoc.measure.aggregation.ILongAggregation;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.extern.slf4j.Slf4j;

/**
 * A `SUM` {@link IAggregation}. It will aggregate as longs, doubles or Strings depending on the inputs.
 */
// https://learn.microsoft.com/en-us/dax/sum-function-dax
@Slf4j
public class SumAggregation implements IAggregation, IDoubleAggregation, ILongAggregation, ICharSequenceAggregation {

	public static final String KEY = "SUM";

	@Override
	public Object aggregate(Object l, Object r) {
		if (l == null) {
			return onlyOne(r);
		} else if (r == null) {
			return onlyOne(l);
		} else if (isLongLike(l) && isLongLike(r)) {
			return aggregateLongs(asLong(l), asLong(r));
		} else if (isDoubleLike(l) && isDoubleLike(r)) {
			return aggregateDoubles(asDouble(l), asDouble(r));
		} else {
			return aggregateObjects(l, r);
		}
	}

	protected Object onlyOne(Object r) {
		if (r == null) {
			return null;
		} else if (isLongLike(r)) {
			return asLong(r);
		} else if (isDoubleLike(r)) {
			return asDouble(r);
		} else if (r instanceof Collection<?> asCollection) {
			return asCollection.stream().filter(Objects::nonNull).collect(Collectors.toSet());
		} else {
			// Wrap into a Set, so this aggregate function return either a long/double, or a String, or Set
			return r.toString();
		}
	}

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

	@Override
	public double aggregateDoubles(double left, double right) {
		return left + right;
	}

	@Override
	public long aggregateLongs(long left, long right) {
		return left + right;
	}

	protected boolean isLongLike(Object o) {
		return AdhocPrimitiveHelpers.isLongLike(o);
	}

	protected long asLong(Object o) {
		return AdhocPrimitiveHelpers.asLong(o);
	}

	protected boolean isDoubleLike(Object o) {
		return AdhocPrimitiveHelpers.isDoubleLike(o);
	}

	protected double asDouble(Object o) {
		return AdhocPrimitiveHelpers.asDouble(o);
	}

	@Override
	public CharSequence aggregateStrings(CharSequence l, CharSequence r) {
		Object aggregate = aggregate(l, r);

		if (aggregate == null) {
			return null;
		} else if (aggregate instanceof CharSequence aggregateCS) {
			return aggregateCS;
		} else {
			throw new IllegalArgumentException("Not a charSequence: " + PepperLogHelper.getObjectAndClass(aggregate));
		}
	}

	@Override
	public long neutralLong() {
		return 0L;
	}
}
