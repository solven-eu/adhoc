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

import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.RequiredArgsConstructor;

/**
 * Enables creating {@link ISliceFilter} instances, enabling to skip or not optimizations.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
public class FilterBuilder {
	final IFilterOptimizerHelpers optimizer;

	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	final List<ISliceFilter> filters = new ArrayList<>();

	final boolean andElseOr;

	public static FilterBuilder and() {
		return new FilterBuilder(AdhocUnsafe.sliceFilterOptimizer, true);
	}

	public static FilterBuilder and(Collection<? extends ISliceFilter> filters) {
		return and().filters(filters);
	}

	public static FilterBuilder and(ISliceFilter first, ISliceFilter second, ISliceFilter... more) {
		return and().filter(first, second, more);
	}

	public static FilterBuilder or() {
		return new FilterBuilder(AdhocUnsafe.sliceFilterOptimizer, false);
	}

	public static FilterBuilder or(Collection<? extends ISliceFilter> filters) {
		return or().filters(filters);
	}

	public static FilterBuilder or(ISliceFilter first, ISliceFilter second, ISliceFilter... more) {
		return or().filter(first, second, more);
	}

	public FilterBuilder filters(Collection<? extends ISliceFilter> filters) {
		this.filters.addAll(filters);

		return this;
	}

	public FilterBuilder filter(ISliceFilter first, ISliceFilter second, ISliceFilter... more) {
		this.filters.add(first);
		this.filters.add(second);
		this.filters.addAll(Arrays.asList(more));

		return this;
	}

	/**
	 * 
	 * @return a {@link ISliceFilter} which is optimized.
	 */
	public ISliceFilter optimize() {
		if (andElseOr) {
			return optimizer.and(filters, false);
		} else {
			return optimizer.or(filters);
		}
	}

	/**
	 * 
	 * @return a {@link ISliceFilter} skipping all optimizations.
	 */
	public ISliceFilter combine() {
		if (filters.isEmpty()) {
			if (andElseOr) {
				return ISliceFilter.MATCH_ALL;
			} else {
				return ISliceFilter.MATCH_NONE;
			}
		} else if (filters.size() == 1) {
			return filters.getFirst();
		} else {
			if (andElseOr) {
				return AndFilter.builder().filters(filters).build();
			} else {
				return OrFilter.builder().filters(filters).build();
			}
		}
	}

}
