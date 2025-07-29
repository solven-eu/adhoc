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
package eu.solven.adhoc.query.table;

import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.filter.ISliceFilter;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;

/**
 * Couple an {@link Aggregator} with an {@link ISliceFilter}, planning for a `FILTER` sql.
 * 
 * https://modern-sql.com/feature/filter
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder(toBuilder = true)
public class FilteredAggregator implements IAliasedAggregator {
	private static final long DEFAULT_INDEX = 0;

	@NonNull
	Aggregator aggregator;

	@NonNull
	@Default
	ISliceFilter filter = ISliceFilter.MATCH_ALL;

	@Default
	long index = DEFAULT_INDEX;

	// An alias to differentiate between same Aggregator with different filter in the same query
	@Override
	public String getAlias() {
		if (index == DEFAULT_INDEX) {
			return aggregator.getName();
		} else {
			return aggregator.getName() + "_" + index;
		}
	}

	public static Aggregator toAggregator(FilteredAggregator fa) {
		return Aggregator.edit(fa.aggregator).name(fa.getAlias()).build();
	}

}
