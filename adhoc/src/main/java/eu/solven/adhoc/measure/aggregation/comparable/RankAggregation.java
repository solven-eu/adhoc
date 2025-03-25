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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.sum.IAggregationCarrier;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Returns the n-th elements.
 */
// https://learn.microsoft.com/fr-fr/sql/t-sql/functions/rank-transact-sql
@Deprecated(since = "NotReady: how would this work with a compositeCube?")
@Builder
public class RankAggregation implements IAggregation, IAggregationCarrier.IHasCarriers {

	public static final String KEY = "RANK";

	public static final String P_RANK = "rank";
	// ASC or DESC
	public static final String P_ORDER = "order";

	private final int rank;
	private final boolean ascElseDesc;

	/**
	 * 
	 * @param rank
	 *            1-based. If 1, we return the maximum. If -1, we return the min.
	 * @return
	 */
	public static RankAggregation fromMax(int rank) {
		if (rank == 0) {
			throw new IllegalArgumentException("rank is 1-based. Can not be `0`");
		} else if (rank > 0) {
			return make(Map.of(P_RANK, rank, P_ORDER, "DESC"));
		} else {
			return make(Map.of(P_RANK, -rank, P_ORDER, "ASC"));
		}
	}

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
			// By default, we order by DESC: `rank(1)` is the maximum.
			ascElseDesc = false;
		} else if (rawOrder instanceof String rawOrderString) {
			if ("ASC".equalsIgnoreCase(rawOrderString)) {
				ascElseDesc = true;
			} else if ("DESC".equalsIgnoreCase(rawOrderString)) {
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
	 * The {@link IAggregationCarrier} for {@link RankAggregation}
	 * 
	 * @author Benoit Lacelle
	 */
	public static interface IRankAggregationCarrier extends IAggregationCarrier {

		/**
		 * 
		 * @param input
		 *            May be any Comparable object, or a {@link IRankAggregationCarrier}.
		 * @return a new IRankAggregationCarrier integrating given
		 */
		IRankAggregationCarrier add(Object input);

		Collection<?> getTopElements();

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
	public static class RankedElementsCarrier implements IRankAggregationCarrier {
		int rank;
		Comparator<Object> comparator;
		// BEWARE Should we have large capacity or not?
		List<Object> topElements;

		public static RankedElementsCarrier empty(int rank, boolean ascElseDesc) {
			Comparator<Object> comparator;
			if (ascElseDesc) {
				comparator = (Comparator) Comparator.naturalOrder();
			} else {
				comparator = (Comparator) Comparator.reverseOrder();
			}

			List<Object> queue = Collections.emptyList();

			return RankedElementsCarrier.builder().rank(rank).comparator(comparator).topElements(queue).build();
		}

		public static IRankAggregationCarrier of(int rank, boolean ascElseDesc, Object first) {
			if (first instanceof IRankAggregationCarrier carrier) {
				return carrier;
			} else {
				return empty(rank, ascElseDesc).add(first);
			}
		}

		public RankedElementsCarrier add(Object element) {
			List<Object> merged;
			if (element instanceof IRankAggregationCarrier otherCarrier) {
				Iterator<?> thisElements = new LinkedList<>(topElements).iterator();
				Iterator<?> otherElements = new LinkedList<>(otherCarrier.getTopElements()).iterator();

				UnmodifiableIterator<Object> mergedIterator =
						Iterators.mergeSorted(Arrays.asList(thisElements, otherElements), this.getComparator());

				merged = new LinkedList<>();

				// Remember previous as `Iterators.mergeSorted` does not de-duplicate
				Object previous = null;
				while (mergedIterator.hasNext() && merged.size() < rank) {
					Object next = mergedIterator.next();

					if (previous == null || !previous.equals(next)) {
						merged.add(next);
						previous = next;
					}
				}
			} else {
				merged = new LinkedList<>(topElements);

				int insertionIndex = Collections.binarySearch((List) topElements, element, comparator);
				if (insertionIndex >= 0) {
					if (insertionIndex > rank) {
						// Skip adding at this element is out of rank
					} else {
						merged.set(insertionIndex, element);
					}
				} else if (insertionIndex < -rank) {
					// Skip adding at this element is out of rank
				} else {
					merged.add(-insertionIndex - 1, element);
				}

				if (merged.size() > rank) {
					merged.removeLast();
				}

			}
			return RankedElementsCarrier.builder().rank(rank).comparator(comparator).topElements(merged).build();

		}

		@Override
		public void acceptValueReceiver(IValueReceiver valueConsumer) {
			if (topElements.size() >= rank) {
				// `-1` as rank is 1-based
				valueConsumer.onObject(topElements.get(rank - 1));
			} else {
				valueConsumer.onObject(null);
			}
		}

	}

	@Builder
	public static class ImmutableRankCarrier implements IRankAggregationCarrier {
		@NonNull
		RankAggregation rankAggregation;

		@NonNull
		Object element;

		@Override
		public void acceptValueReceiver(IValueReceiver valueConsumer) {
			valueConsumer.onObject(element);
		}

		@Override
		public IRankAggregationCarrier add(Object input) {
			return RankedElementsCarrier.of(rankAggregation.rank, rankAggregation.ascElseDesc, element).add(input);
		}

		@Override
		public Collection<?> getTopElements() {
			return Set.of(element);
		}

	}

	@Override
	public IRankAggregationCarrier aggregate(Object l, Object r) {
		if (l == null) {
			return aggregateOne(r);
		} else if (r == null) {
			return aggregateOne(l);
		} else {
			if (l instanceof IRankAggregationCarrier countHolder) {
				return countHolder.add(r);
			} else if (r instanceof IRankAggregationCarrier countHolder) {
				return countHolder.add(l);
			} else {
				return RankedElementsCarrier.empty(rank, ascElseDesc).add(l).add(r);
			}
		}
	}

	protected IRankAggregationCarrier aggregateOne(Object one) {
		if (one instanceof IRankAggregationCarrier asCarrier) {
			return asCarrier;
		} else {
			return RankedElementsCarrier.of(rank, ascElseDesc, one);
		}
	}

	@Override
	public IRankAggregationCarrier wrap(Object v) {
		if (v instanceof RankedElementsCarrier rankCarrier) {
			// Does it happen on Composite cubes?
			return rankCarrier;
		}
		return ImmutableRankCarrier.builder().rankAggregation(this).element(v).build();
	}
}
