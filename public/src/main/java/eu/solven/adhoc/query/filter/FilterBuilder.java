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
package eu.solven.adhoc.query.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder;
import lombok.RequiredArgsConstructor;

/**
 * Enables creating {@link ISliceFilter} instances, enabling to skip or not optimizations.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Builder
public class FilterBuilder {
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	final List<ISliceFilter> filters = new ArrayList<>();

	final Type andElseOr;

	/**
	 * The different type of {@link ISliceFilter} buildable with a {@link FilterBuilder}.
	 */
	public enum Type {
		AND, OR, NOT;
	}

	public static FilterBuilder and() {
		return new FilterBuilder(Type.AND);
	}

	public static FilterBuilder and(Collection<? extends ISliceFilter> filters) {
		return and().filters(filters);
	}

	public static FilterBuilder and(ISliceFilter first, ISliceFilter... more) {
		return and().filter(first, more);
	}

	public static FilterBuilder or() {
		return new FilterBuilder(Type.OR);
	}

	public static FilterBuilder or(Collection<? extends ISliceFilter> filters) {
		return or().filters(filters);
	}

	public static FilterBuilder or(ISliceFilter first, ISliceFilter... more) {
		return or().filter(first, more);
	}

	public static FilterBuilder not(ISliceFilter filter) {
		return new FilterBuilder(Type.NOT).filter(filter);
	}

	public FilterBuilder filters(Collection<? extends ISliceFilter> filters) {
		this.filters.addAll(filters);

		return this;
	}

	public FilterBuilder filter(ISliceFilter first, ISliceFilter... more) {
		this.filters.add(first);
		this.filters.addAll(Arrays.asList(more));

		return this;
	}

	/**
	 * 
	 * @return a {@link ISliceFilter} which is optimized.
	 */
	public ISliceFilter optimize() {
		return optimize(AdhocUnsafe.filterOptimizer);
	}

	public ISliceFilter optimize(IFilterOptimizer optimizer) {
		if (andElseOr == Type.AND) {
			return optimizer.and(filters, false);
		} else if (andElseOr == Type.OR) {
			return optimizer.or(filters);
		} else {
			// if (andElseOr == Type.NOT)
			if (filters.size() != 1) {
				throw new IllegalStateException("NOT is applicable to exactly 1 filter. Was: " + filters);
			}
			return optimizer.not(filters.getFirst());
		}
	}

	/**
	 * 
	 * 
	 * 
	 * @return a {@link ISliceFilter} skipping all optimizations.
	 */
	public ISliceFilter combine() {
		// Keep trivial optimizations as in various occasions, we may use `.combine` as we rely on optimized expressions
		// but we tend to forget about `.matchAll` edge-case
		removeTrivialFilters();

		if (filters.isEmpty()) {
			if (andElseOr == Type.AND) {
				return ISliceFilter.MATCH_ALL;
			} else if (andElseOr == Type.OR) {
				return ISliceFilter.MATCH_NONE;
			} else {
				throw new IllegalStateException("NOT is applicable to exactly 1 filter. Was: " + filters);
			}
		} else if (filters.size() == 1) {
			ISliceFilter singleFilter = filters.getFirst();
			if (andElseOr == Type.NOT) {
				return NotFilter.builder().negated(singleFilter).build();
			} else {
				return singleFilter;
			}
		} else {
			if (andElseOr == Type.AND) {
				return AndFilter.builder().ands(filters).build();
			} else if (andElseOr == Type.OR) {
				return OrFilter.builder().ors(filters).build();
			} else {
				throw new IllegalStateException("NOT is applicable to exactly 1 filter. Was: " + filters);
			}
		}
	}

	protected void removeTrivialFilters() {
		if (andElseOr == Type.AND) {
			if (filters.contains(ISliceFilter.MATCH_NONE)) {
				filters.clear();
				filters.add(ISliceFilter.MATCH_NONE);
			} else {
				filters.removeIf(ISliceFilter.MATCH_ALL::equals);
			}
		} else if (andElseOr == Type.OR) {
			if (filters.contains(ISliceFilter.MATCH_ALL)) {
				filters.clear();
				filters.add(ISliceFilter.MATCH_ALL);
			} else {
				filters.removeIf(ISliceFilter.MATCH_NONE::equals);
			}
		}
	}

}
