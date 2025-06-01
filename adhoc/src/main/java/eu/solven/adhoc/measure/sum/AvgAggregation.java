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

import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier;
import eu.solven.adhoc.measure.aggregation.comparable.RankAggregation;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

/**
 * Returns the n-th elements.
 */
// https://learn.microsoft.com/fr-fr/sql/t-sql/functions/rank-transact-sql
public class AvgAggregation implements IAggregation, IAggregationCarrier.IHasCarriers {

	public static final String KEY = "AVG";

	/**
	 * The {@link IAggregationCarrier} for {@link RankAggregation}
	 * 
	 * @author Benoit Lacelle
	 */
	public static interface IAvgAggregationCarrier extends IAggregationCarrier {

		/**
		 * 
		 * @param input
		 *            May be any Comparable object, or a {@link IAvgAggregationCarrier}.
		 * @return a new IRankAggregationCarrier integrating given
		 */
		IAvgAggregationCarrier add(Object input);

		long getCount();

		long getSumLong();

		double getSumDouble();

	}

	/**
	 * This class holds the top elements, enough to get the ranked elements.
	 * 
	 * It is immutable;
	 * 
	 * @author Benoit Lacelle
	 */
	@Value
	@Builder
	public static class AvgCarrier implements IAvgAggregationCarrier {
		@Default
		long count = 0;

		@Default
		long sumLong = 0;
		@Default
		double sumDouble = 0D;

		public static AvgCarrier empty() {
			return AvgCarrier.builder().build();
		}

		public static IAvgAggregationCarrier of(Object first) {
			if (first instanceof IAvgAggregationCarrier carrier) {
				return carrier;
			} else {
				return empty().add(first);
			}
		}

		public AvgCarrier add(Object element) {
			if (element instanceof IAvgAggregationCarrier otherCarrier) {
				return AvgCarrier.builder()
						.count(count + otherCarrier.getCount())
						.sumLong(sumLong + otherCarrier.getSumLong())
						.sumDouble(sumDouble + otherCarrier.getSumDouble())
						.build();
			} else if (AdhocPrimitiveHelpers.isLongLike(element)) {
				long asLong = AdhocPrimitiveHelpers.asLong(element);
				return AvgCarrier.builder().count(count + 1).sumLong(sumLong + asLong).sumDouble(sumDouble).build();
			} else if (AdhocPrimitiveHelpers.isDoubleLike(element)) {
				double asDouble = AdhocPrimitiveHelpers.asDouble(element);
				return AvgCarrier.builder().count(count + 1).sumLong(sumLong).sumDouble(sumDouble + asDouble).build();
			} else {
				throw new IllegalArgumentException("Can not AVG around `%s`".formatted(element));
			}

		}

		@Override
		public void acceptValueReceiver(IValueReceiver valueConsumer) {
			if (count == 0) {
				valueConsumer.onLong(0);
			} else if (sumDouble == 0D) {
				if (sumLong % count == 0) {
					valueConsumer.onLong(sumLong / count);
				} else {
					valueConsumer.onDouble(1D * sumLong / count);
				}
			} else {
				valueConsumer.onDouble((sumDouble + sumLong) / count);
			}
		}
	}

	@Override
	public IAvgAggregationCarrier aggregate(Object l, Object r) {
		if (l == null) {
			return aggregateOne(r);
		} else if (r == null) {
			return aggregateOne(l);
		} else {
			if (l instanceof IAvgAggregationCarrier countHolder) {
				return countHolder.add(r);
			} else if (r instanceof IAvgAggregationCarrier countHolder) {
				return countHolder.add(l);
			} else {
				return AvgCarrier.empty().add(l).add(r);
			}
		}
	}

	protected IAvgAggregationCarrier aggregateOne(Object one) {
		if (one == null) {
			return null;
		} else if (one instanceof IAvgAggregationCarrier asCarrier) {
			return asCarrier;
		} else {
			return AvgCarrier.of(one);
		}
	}

	@Override
	public IAvgAggregationCarrier wrap(Object v) {
		if (v instanceof AvgCarrier rankCarrier) {
			// Does it happen on Composite cubes?
			return rankCarrier;
		}
		return AvgCarrier.of(v);
	}

	public static boolean isAvg(String aggregationKey) {
		return KEY.equals(aggregationKey) || AvgAggregation.class.getName().equals(aggregationKey);
	}
}
