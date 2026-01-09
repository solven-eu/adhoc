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
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.IDoubleAggregation;
import eu.solven.adhoc.measure.aggregation.ILongAggregation;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.extern.slf4j.Slf4j;

/**
 * A `SUM` {@link IAggregation}. It will aggregate as longs, doubles or Object depending on the inputs.
 * <p>
 * The {@link Object} case differs from usual implementations. As being an in-memory engine, it feels preferable to
 * accumulate {@link Object} in a {@link List}, rather than concatenating their {@link Object#toString()}.
 * <p>
 * More specifically: it will aggregate as long if it encounters only long-like. It would switch to a double if it
 * encounter any double-like. It would collect not double-like into a List, wrapping the whole into a XXX.
 *
 * @author Benoit Lacelle
 */
// https://learn.microsoft.com/en-us/dax/sum-function-dax
@Slf4j
public class SumAggregation implements IAggregation, IDoubleAggregation, ILongAggregation {

	public static final String KEY = "SUM";

	private static final int MAX_COLLECTION_SIZE = 16 * 1024;

	public static boolean isSum(String operator) {
		return "+".equals(operator) || KEY.equals(operator) || operator.equals(SumAggregation.class.getName());
	}

	protected int limitSize() {
		return MAX_COLLECTION_SIZE;
	}

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
			// TODO Prevent auto-boxing again (Long -> long -> Long)
			return asLong(r);
		} else if (isDoubleLike(r)) {
			// TODO Prevent auto-boxing again (Double -> double -> Double)
			return asDouble(r);
		} else {
			return wrapNotANumber(r);
		}
	}

	protected Object wrapNotANumber(Object r) {
		if (r instanceof Throwable t) {
			return t;
		} else if (r instanceof Collection<?> asCollection) {
			return asCollection.stream().filter(Objects::nonNull).collect(ImmutableList.toImmutableList());
		} else if (r instanceof CharSequence charSequence) {
			return charSequence.toString();
		} else {
			// Wrap into a List, so this aggregate function return either a long/double, or Set
			return List.of(r);
		}
	}

	@SuppressWarnings("checkstyle:MagicNumber")
	protected Object aggregateObjects(Object l, Object r) {
		if (l instanceof Throwable t) {
			return t;
		} else if (r instanceof Throwable t) {
			return t;
		}
		return aggregateCollections(wrapAsCollection(l), wrapAsCollection(r));
	}

	protected Collection<?> wrapAsCollection(Object o) {
		if (o instanceof Collection<?> c) {
			// Automated unnesting: if this behavior is not satisfying, you should probably rely on a custom
			// IAggregation.
			return ImmutableList.copyOf(c);
		}
		return ImmutableList.of(o);
	}

	protected Collection<?> aggregateCollections(Collection<?> left, Collection<?> right) {
		int limitSize = checkCollectionsSizes(left, right);

		return Stream.concat(left.stream(), right.stream()).limit(limitSize).collect(ImmutableList.toImmutableList());
	}

	protected int checkCollectionsSizes(Collection<?> left, Collection<?> right) {
		int limitSize = limitSize();

		int leftSize = left.size();
		int rightSize = right.size();
		if (leftSize + rightSize > limitSize) {
			if (AdhocUnsafe.isFailFast()) {
				throw new IllegalArgumentException("Can not aggregate more than %s elements".formatted(limitSize));
			} else {
				log.warn("left.size() + right.size() > limit ({} + {} > {}). Truncating the result",
						leftSize,
						rightSize,
						limitSize);
			}
		}
		return limitSize;
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
	public long neutralLong() {
		return 0L;
	}

	@Override
	public double neutralDouble() {
		return 0D;
	}
}
