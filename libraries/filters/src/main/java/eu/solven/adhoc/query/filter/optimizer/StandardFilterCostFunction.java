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
package eu.solven.adhoc.query.filter.optimizer;

import eu.solven.adhoc.query.filter.IAndFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.filter.INotFilter;
import eu.solven.adhoc.query.filter.IOrFilter;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.util.NotYetImplementedException;

/**
 * Default {@link IFilterCostFunction}.
 * 
 * @author Benoit Lacelle
 */
public class StandardFilterCostFunction implements IFilterCostFunction {

	/**
	 * Given a formula, the cost can be evaluated manually by counting the number of each operators. `AND` is free, `OR`
	 * cost `2`, `!` cost 3, any value matcher costs `1`.
	 * 
	 * @param f
	 * @return
	 */
	// factors are additive (hence not multiplicative) as we prefer a high `Not` (counting once) than multiple deep Not
	@SuppressWarnings("checkstyle:MagicNumber")
	@Override
	public long cost(ISliceFilter f) {
		if (f instanceof IAndFilter andFilter) {
			return cost(andFilter.getOperands());
		} else if (f instanceof INotFilter notFilter) {
			// We generally do not like NOT
			// But it one prefers `!(a|b|c|d)` over `!a&!b&!c&!d`
			return 2 * cost(notFilter.getNegated());
		} else if (f instanceof IColumnFilter columnFilter) {
			// `Not` costs 3: we prefer one OR than one NOT
			return cost(columnFilter.getValueMatcher());
		} else if (f instanceof IOrFilter orFilter) {
			// We generally do not like NOT
			// But it one prefers `!(a|b|c|d)` over `!a&!b&!c&!d`
			return 5 + cost(orFilter.getOperands());
		} else {
			throw new NotYetImplementedException("Not managed: %s".formatted(f));
		}
	}

	@SuppressWarnings("checkstyle:MagicNumber")
	@Override
	public long cost(IValueMatcher m) {
		if (m instanceof NotMatcher notMatcher) {
			IValueMatcher negated = notMatcher.getNegated();
			if (negated instanceof EqualsMatcher || negated instanceof InMatcher) {
				// `=out=` is not very bad, but still more costly than `!(=out=)`
				return 5;
			} else {
				return 7 + cost(negated);
			}
		} else {
			return 3;
		}
	}

}
