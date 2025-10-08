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
package eu.solven.adhoc.query.filter.stripper;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;

import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.filter.INotFilter;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.value.AndMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import lombok.RequiredArgsConstructor;

/**
 * Default implementation of IFilterStripper
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
public class FilterStripper implements IFilterStripper {
	protected final ISliceFilter where;

	// Splitting `WHERE` is memoized as it is called many times, as `isStricterThan` is recursive.
	// TODO Should we merge in a single supplier?
	protected final Supplier<Set<ISliceFilter>> whereAnds = Suppliers.memoize(this::splitWhereAnd);
	protected final Supplier<Set<ISliceFilter>> whereOrs = Suppliers.memoize(this::splitWhereOr);

	protected Set<ISliceFilter> splitWhereAnd() {
		return FilterHelpers.splitAnd(where);
	}

	protected Set<ISliceFilter> splitWhereOr() {
		return FilterHelpers.splitOr(where);
	}

	protected IFilterStripper makeStripper(ISliceFilter subWhere) {
		return new FilterStripper(subWhere);
	}

	@Override
	public ISliceFilter strip(ISliceFilter filter) {
		if (where.isMatchAll()) {
			// `WHERE` has no clause: `FILTER` has to keep all clauses
			return filter;
		} else if (isStricterThan(filter)) {
			// Catch some edge-case like `where.equals(filter)`
			// More generally: if `WHERE && FILTER === WHERE`, then `FILTER` is irrelevant
			return ISliceFilter.MATCH_ALL;
		}

		// Given the FILTER, we reject the AND operands already covered by WHERE
		ISliceFilter postAnd;
		{

			// Split the FILTER in parts
			Set<? extends ISliceFilter> andOperands = FilterHelpers.splitAnd(filter);

			Set<ISliceFilter> notInWhere = new LinkedHashSet<>();

			boolean singleAnd = andOperands.size() == 1;

			// For each part of `FILTER`, reject those already filtered in `WHERE`
			for (ISliceFilter subFilter : andOperands) {
				ISliceFilter simplerSubFilter;
				if (singleAnd) {
					// Break cycle when we reach simplest filters
					simplerSubFilter = subFilter;
				} else {
					simplerSubFilter = strip(subFilter);
				}

				if (andMatchNone(simplerSubFilter)) {
					return ISliceFilter.MATCH_NONE;
				} else {
					boolean whereCoversSubFilter = isStricterThan(simplerSubFilter);

					if (!whereCoversSubFilter) {
						notInWhere.add(simplerSubFilter);
					}
				}
			}
			// `.combine` to break cycle, and output has not reason to be more optimized than input
			postAnd = FilterBuilder.and(notInWhere).combine();
		}

		// Given the FILTER, we reject the OR operands already rejected by WHERE
		// TODO Why managing OR after AND? (and not the inverse)
		ISliceFilter postOr;
		{

			Set<ISliceFilter> orOperands = FilterHelpers.splitOr(postAnd);
			boolean singleOr = orOperands.size() == 1;

			Set<ISliceFilter> notInWhere = new LinkedHashSet<>();
			// For each part of `FILTER`, reject those already filtered in `WHERE`
			for (ISliceFilter subFilter : orOperands) {

				ISliceFilter simplerSubFilter;
				if (singleOr) {
					// Break cycle when we reach simplest filters
					simplerSubFilter = subFilter;
				} else {
					simplerSubFilter = strip(subFilter);
				}

				if (andMatchNone(simplerSubFilter)) {
					continue;
				} else {
					boolean whereRejectsSubFilter = isStricterThan(simplerSubFilter);

					if (!whereRejectsSubFilter) {
						notInWhere.add(simplerSubFilter);
					}
				}
			}

			// `.combine` to break cycle, and output has not reason to be more optimized than input
			postOr = FilterBuilder.or(notInWhere).combine();
		}

		return postOr;
	}

	// `a&b=matchNone` is equivalent to `a includes !b`, which is equivalent to `a is stricter than !b`
	protected boolean andMatchNone(ISliceFilter right) {
		return isStricterThan(right.negate());
	}

	@Override
	public boolean isStricterThan(ISliceFilter laxer) {
		if (where instanceof INotFilter notStricter && laxer instanceof INotFilter notLaxer) {
			return makeStripper(notLaxer.getNegated()).isStricterThan(notStricter.getNegated());
		} else if (where instanceof IColumnFilter stricterColumn && laxer instanceof IColumnFilter laxerFilter) {
			boolean isSameColumn = stricterColumn.getColumn().equals(laxerFilter.getColumn());
			if (!isSameColumn) {
				return false;
			}
			return isStricterThan(stricterColumn.getValueMatcher(), laxerFilter.getValueMatcher());
		}

		// BEWARE Do not rely on `OrFilter` as this method is called by `AndFilter` optimizations and `OrFilter` also
		// relies on `AndFilter` optimization. Doing so would lead to cycle in the optimizations.
		// BEWARE Do not rely on AndFilter either, else it may lead on further cycles
		// return FilterOptimizerHelpers.and(stricter, laxer).equals(stricter);

		{
			Set<ISliceFilter> allStricters = whereAnds.get();
			Set<ISliceFilter> allLaxers = FilterHelpers.splitAnd(laxer);

			if (allStricters.containsAll(allLaxers)) {
				// true if `stricter:A=a1&B=b1` and `laxer:B=b1`
				return true;
			}
			// true if `a==a1&b==b1&c==c1` and `a=in=(a1,a2)&b=in=(b1,b2)`.
			boolean allLaxersHasStricter = allLaxers.stream().allMatch(oneLaxer -> {
				if (allStricters.contains(oneLaxer)) {
					// Fast track for `a==a1` in `a==a1&b==b1&c==c1` and `a==a1&b=in=(b1,b2)`
					return true;
				}

				return allStricters.stream().anyMatch(oneStricter -> {
					if (oneStricter.equals(where) && oneLaxer.equals(laxer)) {
						// break cycle in recursivity
						return false;
					}

					return makeStripper(oneStricter).isStricterThan(oneLaxer);
				});
			});
			if (allLaxersHasStricter) {
				return true;
			}
		}

		{
			Set<ISliceFilter> allStricters = whereOrs.get();
			Set<ISliceFilter> allLaxers = FilterHelpers.splitOr(laxer);
			if (allLaxers.containsAll(allStricters)) {
				// true if `stricter:A=a1` and `laxer:A=a1|B=b1`.
				return true;
			}

			// true if `stricter:A=7` and `laxer:A>5|B=b1`.
			boolean allLaxersInStricter = allStricters.stream().allMatch(oneStricter -> {
				if (allLaxers.contains(oneStricter)) {
					// Fast track for `a==a1` in `a==a1&b==b1&c==c1` and `a==a1&b=in=(b1,b2)`
					return true;
				}

				return allLaxers.stream().anyMatch(oneLaxer -> {
					if (oneStricter.equals(where) && oneLaxer.equals(laxer)) {
						// break cycle in recursivity
						return false;
					}

					return makeStripper(oneStricter).isStricterThan(oneLaxer);
				});
			});
			if (allLaxersInStricter) {
				return true;
			}
		}

		return false;
	}

	/**
	 * 
	 * @param stricter
	 * @param laxer
	 * @return true if stricter {@link IValueMatcher} matches a subset of objects than laxer.
	 */
	protected boolean isStricterThan(IValueMatcher stricter, IValueMatcher laxer) {
		return AndMatcher.and(stricter, laxer).equals(stricter);
	}
}
