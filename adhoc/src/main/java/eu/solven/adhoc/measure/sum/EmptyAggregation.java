/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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

import java.util.Set;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.IDoubleAggregation;
import eu.solven.adhoc.measure.aggregation.ILongAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.EmptyMeasure;
import eu.solven.adhoc.query.table.IAliasedAggregator;

/**
 * Relates with {@link EmptyMeasure}. Useful to materialize an {@link IAggregation} to force the DAG not to be empty
 * when querying the table. It helps materializing the relevant slices, without requesting any aggregation.
 * 
 * @author Benoit Lacelle
 */
public class EmptyAggregation implements IAggregation, ILongAggregation, IDoubleAggregation {

	public static final String KEY = "EMPTY";

	@Override
	public Object aggregate(Object left, Object right) {
		// BEWARE SHould we throw?
		return null;
	}

	@Override
	public long aggregateLongs(long left, long right) {
		// BEWARE SHould we throw?
		return 0;
	}

	@Override
	public double aggregateDoubles(double left, double right) {
		// BEWARE SHould we throw?
		return 0;
	}

	@Override
	public long neutralLong() {
		return 0;
	}

	/**
	 * 
	 * @param aggregationKey
	 * @return true if the aggregationKey refers to {@link EmptyAggregation}
	 */
	public static boolean isEmpty(String aggregationKey) {
		return EmptyAggregation.KEY.equals(aggregationKey) || aggregationKey.equals(EmptyAggregation.class.getName());
	}

	/**
	 * 
	 * @param aggregator
	 * @return true if the {@link Aggregator} aggregationKey refers to {@link EmptyAggregation}
	 */
	public static boolean isEmpty(Aggregator aggregator) {
		return isEmpty(aggregator.getAggregationKey());
	}

	public static boolean isEmpty(Set<? extends IAliasedAggregator> aggregators) {
		boolean hasEmpty = aggregators.stream().map(aa -> aa.getAggregator()).anyMatch(EmptyAggregation::isEmpty);

		if (hasEmpty && aggregators.size() >= 2) {
			throw new IllegalArgumentException("Must not query must empty and non-empty: " + aggregators);
		}

		return hasEmpty;
	}

}
