/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.column;

import java.util.Collection;
import java.util.Set;
import java.util.function.UnaryOperator;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.table.transcoder.AliasingContext;
import lombok.experimental.UtilityClass;

/**
 * Transcodes {@link FilteredAggregator}s to their table-side equivalent (column name swap + filter rewrite).
 *
 * @author Benoit Lacelle
 */
@UtilityClass
public class FilteredAggregatorTranscoder {

	/**
	 * @return rewritten {@link FilteredAggregator}s: each aggregator's column is mapped through
	 *         {@code transcodingContext.underlying(...)} and its filter through {@code filterTranscoder}.
	 */
	public static Collection<? extends FilteredAggregator> transcode(Set<FilteredAggregator> aggregators,
			AliasingContext transcodingContext,
			UnaryOperator<ISliceFilter> filterTranscoder) {
		return aggregators.stream().map(filteredAggregator -> {
			Aggregator aggregator = filteredAggregator.getAggregator();
			Aggregator transcodedAggregator = aggregator.toBuilder()
					.columnName(transcodingContext.underlying(aggregator.getColumnName()))
					.build();
			return filteredAggregator.toBuilder()
					.aggregator(transcodedAggregator)
					.filter(filterTranscoder.apply(filteredAggregator.getFilter()))
					.build();
		}).collect(ImmutableSet.toImmutableSet());
	}
}
