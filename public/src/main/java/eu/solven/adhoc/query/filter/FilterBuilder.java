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

import lombok.RequiredArgsConstructor;

/**
 * Enables creating {@link ISliceFilter} instances, enabling to skip or not optimizations.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
public class FilterBuilder {
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	final List<ISliceFilter> filters = new ArrayList<>();

	final boolean andElseOr;

	public static FilterBuilder and() {
		return new FilterBuilder(true);
	}

	public static FilterBuilder and(Collection<? extends ISliceFilter> filters) {
		return and().filters(filters);
	}

	public static FilterBuilder and(ISliceFilter first, ISliceFilter second, ISliceFilter... more) {
		return and().filter(first, second, more);
	}

	public static FilterBuilder or() {
		return new FilterBuilder(false);
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
			return FilterOptimizerHelpers.and(filters, false);
		} else {
			return or2(filters);
		}
	}

	/**
	 * 
	 * @return a {@link ISliceFilter} skipping most expensive optimizations.
	 */
	public ISliceFilter combine() {
		// TODO If size --1, return the insle filter
		if (andElseOr) {
			return AndFilter.builder().filters(filters).build();
		} else {
			return OrFilter.builder().filters(filters).build();
		}
	}

	// `first, second, more` syntax to push providing at least 2 arguments
	// public static ISliceFilter and(ISliceFilter first, ISliceFilter second, ISliceFilter... more) {
	// if (more.length == 0 && first.equals(second)) {
	// return first;
	// }
	// return and(Lists.asList(first, second, more));
	// }
	//
	// public static ISliceFilter and(Collection<? extends ISliceFilter> filters) {
	// return and(filters, false);
	// }

	private static ISliceFilter or2(Collection<? extends ISliceFilter> filters) {
		// OR relies on AND optimizations
		List<ISliceFilter> negated = filters.stream().map(NotFilter::not).toList();

		ISliceFilter negatedOptimized = FilterOptimizerHelpers.and(negated, true);

		if (negatedOptimized instanceof INotFilter notFilter) {
			return notFilter.getNegated();
		} else {
			return NotFilter.not(negatedOptimized);
		}
	}

}
