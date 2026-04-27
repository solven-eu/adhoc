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
import eu.solven.adhoc.measure.model.IAliasedAggregator;

/**
 * Relates with {@link EmptyMeasure}. Useful to materialize an {@link IAggregation} to force the DAG not to be empty
 * when querying the table. It helps materializing the relevant slices, without requesting any aggregation.
 *
 * <p>
 * <b>Wire semantics — behaves as a NULL column.</b> An aggregator backed by {@code EmptyAggregation} surfaces as a
 * column whose value is always {@code null} (analogous to {@code SELECT NULL AS x …} in SQL). Each record produced by
 * the table layer carries the aggregator's alias as a key, with {@code null} as the value, whenever the aggregator's
 * per-aggregator {@code FILTER} matches the row. The aggregator therefore contributes only its slice's existence —
 * never a value — and can coexist with real aggregators in the same {@link TableQueryV4} (the record's other columns
 * carry the real aggregators' values, exactly like a SQL row with a mix of {@code SUM(b)} and {@code NULL AS x}).
 *
 * <p>
 * Two consequences:
 * <ul>
 * <li>An all-empty {@link TableQueryV4} (every aggregator is an {@code EmptyAggregation}) collapses to "list distinct
 * slices", which is the historical use case (materialize coordinates for a measure-less query).</li>
 * <li>A mixed {@link TableQueryV4} (some empty, some real) emits one record per row, where each record carries both the
 * real aggregators' values and the empty aggregators' {@code null} markers — consumers that already tolerate a missing
 * key for a non-applicable aggregator must also tolerate the key being present with a {@code
 * null} value.</li>
 * </ul>
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
	public long neutralLong() {
		return 0;
	}

	@Override
	public double aggregateDoubles(double left, double right) {
		// BEWARE SHould we throw?
		return 0;
	}

	@Override
	public double neutralDouble() {
		return 0D;
	}

	/**
	 * 
	 * @param aggregationKey
	 * @return true if the aggregationKey refers to {@link EmptyAggregation}
	 */
	public static boolean isEmpty(String aggregationKey) {
		return KEY.equals(aggregationKey) || aggregationKey.equals(EmptyAggregation.class.getName());
	}

	/**
	 * 
	 * @param aggregator
	 * @return true if the {@link Aggregator} aggregationKey refers to {@link EmptyAggregation}
	 */
	public static boolean isEmpty(Aggregator aggregator) {
		return isEmpty(aggregator.getAggregationKey());
	}

	/**
	 * @return {@code true} when EVERY aggregator is an {@link EmptyAggregation} (or the set is empty); {@code false}
	 *         when none are empty OR when empty and non-empty aggregators are mixed. Mixed sets are no longer rejected:
	 *         callers that compute aggregator columns must filter out empty ones via {@link #isEmpty(Aggregator)} when
	 *         iterating per-aggregator. The empty aggregator's only contract is to materialize slices, which the
	 *         non-empty raw-row pass already does — so mixing is a no-op for the empty side.
	 */
	public static boolean isEmpty(Set<? extends IAliasedAggregator> aggregators) {
		long emptyCount =
				aggregators.stream().map(IAliasedAggregator::getAggregator).filter(EmptyAggregation::isEmpty).count();
		// All-or-nothing: only true when every aggregator is empty.
		return emptyCount > 0 && emptyCount == aggregators.size();
	}

}
