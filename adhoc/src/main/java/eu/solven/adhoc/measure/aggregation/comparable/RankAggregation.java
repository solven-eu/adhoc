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
package eu.solven.adhoc.measure.aggregation.comparable;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.sum.IAggregationCarrier;
import eu.solven.adhoc.storage.IValueReceiver;
import lombok.Builder;
import lombok.Value;

/**
 * Returns the n-th elements.
 */
@Deprecated(since = "NotReady: how would this work with a compositeCube?")
@Builder
public class RankAggregation implements IAggregation, IAggregationCarrier.IHasCarriers {

	public static final String KEY = "RANK";

	public static final String P_RANK = "rank";
	// ASC or DESC
	public static final String P_ORDER = "order";

	private final int rank;
	private final boolean ascElseDesc;

	public static RankAggregation make(Map<String, ?> options) {
		int rank;

		Object rawRank = options.get(P_RANK);
		if (rawRank instanceof Number rankAsNumber) {
			rank = rankAsNumber.intValue();
		} else if (rawRank instanceof String rankAsString) {
			rank = Integer.parseInt(rankAsString);
		} else {
			throw new IllegalArgumentException("%s (int) is mandatory".formatted(P_RANK));
		}

		boolean ascElseDesc;
		Object rawOrder = options.get(P_ORDER);
		if (rawOrder == null) {
			ascElseDesc = true;
		} else if (rawOrder instanceof String rawOrderString) {
			if ("ASC".equalsIgnoreCase(rawOrderString)) {
				ascElseDesc = true;
			} else if ("ASC".equalsIgnoreCase(rawOrderString)) {
				ascElseDesc = false;
			} else {
				throw new IllegalArgumentException("%s=%s in invalid. Expected 'ASC' or 'DECS'".formatted(P_ORDER));
			}
		} else {
			throw new IllegalArgumentException("%s (['ASC', 'DECS']) is mandatory".formatted(P_ORDER));
		}

		return RankAggregation.builder().rank(rank).ascElseDesc(ascElseDesc).build();
	}

	/**
	 * This class holds the count. It is useful to differentiate as input long (which count as `1`) and a count.
	 * 
	 * @author Benoit Lacelle
	 */
	@Value
	@Builder
	public static class RankCarrier implements IAggregationCarrier {
		int rank;
		Comparator<Object> comparator;
		// BEWARE Should we have large capacity or not?
		List<Object> elements;

		public static RankCarrier empty(int rank, boolean ascElseDesc) {
			Comparator<Object> comparator;
			if (ascElseDesc) {
				comparator = (Comparator) Comparator.naturalOrder();
			} else {
				comparator = (Comparator) Comparator.reverseOrder();
			}

			List<Object> queue = new LinkedList<>();

			return RankCarrier.builder().rank(rank).comparator(comparator).elements(queue).build();
		}

		public static RankCarrier of(int rank, boolean ascElseDesc, Object first) {
			return empty(rank, ascElseDesc).add(first);
		}

		public RankCarrier add(Object element) {
			List<Object> copy = new LinkedList<>(elements);

			int insertionIndex = Collections.binarySearch((List) elements, comparator);
			if (insertionIndex >= 0) {
				if (insertionIndex > rank) {
					// Skip adding at this element is out of rank
				} else {
					copy.set(insertionIndex, element);
				}
			} else {
				copy.add(-insertionIndex - 1, element);
			}

			if (copy.size() > rank) {
				copy.removeLast();
			}

			return RankCarrier.builder().rank(rank).comparator(comparator).elements(copy).build();
		}

		@Override
		public void acceptValueConsumer(IValueReceiver valueConsumer) {
			valueConsumer.onObject(elements.get(rank));
		}

	}

	@Builder
	public static class ImmutableRankCarrier implements IAggregationCarrier {
		Object element;

		@Override
		public void acceptValueConsumer(IValueReceiver valueConsumer) {
			valueConsumer.onObject(element);
		}

	}

	@Override
	public Object aggregate(Object l, Object r) {
		if (l == null) {
			return aggregateOne(r);
		} else if (r == null) {
			return aggregateOne(l);
		} else {
			if (l instanceof RankCarrier countHolder) {
				return countHolder.add(r);
			} else if (r instanceof RankCarrier countHolder) {
				return countHolder.add(l);
			} else {
				return RankCarrier.empty(rank, ascElseDesc).add(l).add(r);
			}
		}
	}

	protected Object aggregateOne(Object one) {
		if (one instanceof RankCarrier) {
			return one;
		} else {
			return RankCarrier.of(rank, ascElseDesc, one);
		}
	}

	@Override
	public IAggregationCarrier wrap(Object v) {
		if (v instanceof RankCarrier rankCarrier) {
			// Does it happen on Composite cubes?
			return rankCarrier;
		}
		return ImmutableRankCarrier.builder().element(v).build();
	}
}
