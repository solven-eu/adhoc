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
package eu.solven.adhoc.query.filter;

import eu.solven.adhoc.query.filter.value.NotMatcher;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * A boolean `not`/`!`.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
@Jacksonized
public class NotFilter implements INotFilter {

	@NonNull
	final IAdhocFilter negated;

	@Override
	public boolean isNot() {
		return true;
	}

	@Override
	public String toString() {
		return "!(%s)".formatted(negated);
	}

	public static IAdhocFilter not(IAdhocFilter filter) {
		if (filter.isMatchAll()) {
			return MATCH_NONE;
		} else if (filter.isMatchNone()) {
			return MATCH_ALL;
		} else if (filter.isNot() && filter instanceof NotFilter notFilter) {
			return notFilter.getNegated();
		} else if (filter.isColumnFilter() && filter instanceof ColumnFilter columnFilter) {
			// Prefer `c!=c1` over `!(c==c1)`
			return columnFilter.toBuilder().matching(NotMatcher.not(columnFilter.getValueMatcher())).build();
		} else if (filter instanceof OrFilter orFilter) {
			// Plays optimizations given a And of Not.
			// We may prefer `c!=c1&d==d2` over `!(c==c1|d!=d2)`
			return AndFilter.and(orFilter.getOperands().stream().map(NotFilter::not).toList());
		}
		return NotFilter.builder().negated(filter).build();
	}

}