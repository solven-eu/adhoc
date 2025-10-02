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

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;
import lombok.Builder;

/**
 * Like {@link FilterHelpers} but enabling a custom {@link IFilterOptimizer}.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class FilterUtility {
	final IFilterOptimizer optimizer;

	public ISliceFilter commonAnd(Set<? extends ISliceFilter> filters) {
		if (filters.isEmpty()) {
			return ISliceFilter.MATCH_ALL;
		} else if (filters.size() == 1) {
			return filters.iterator().next();
		}

		Iterator<? extends ISliceFilter> iterator = filters.iterator();
		// Common parts are initialized with all parts of the first filter
		Set<ISliceFilter> commonParts = new LinkedHashSet<>(FilterHelpers.splitAnd(iterator.next()));

		while (iterator.hasNext()) {
			Set<ISliceFilter> nextFilterParts = new LinkedHashSet<>(FilterHelpers.splitAnd(iterator.next()));

			commonParts = Sets.intersection(commonParts, nextFilterParts);
		}

		return FilterBuilder.and(commonParts).optimize(optimizer);
	}

	public ISliceFilter commonOr(ImmutableSet<? extends ISliceFilter> filters) {
		// common `OR` in `a|b` and `a|c` is related with negation of the common `AND` between `!a&!b` and `!a&!c`
		return commonAnd(filters.stream().map(ISliceFilter::negate).collect(Collectors.toSet())).negate();
	}

}
