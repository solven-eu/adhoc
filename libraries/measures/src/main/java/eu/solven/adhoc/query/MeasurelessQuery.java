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
package eu.solven.adhoc.query;

import org.jspecify.annotations.Nullable;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.step.IWhereGroupByQuery;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.query.IGroupBy;
import eu.solven.adhoc.model.query.IHasCustomMarker;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.options.IQueryOption;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

/**
 * Typically used to group {@link Aggregator} together, given they are associated to the same filter, groupBy, etc
 * clauses.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
// Builder fields populated via chained setters before .build(); NullAway can't see the cross-method init.
@SuppressWarnings("NullAway.Init")
public class MeasurelessQuery implements IWhereGroupByQuery, IHasCustomMarker, IHasQueryOptions {

	@NonNull
	ISliceFilter filter;

	@NonNull
	IGroupBy groupBy;

	@Nullable
	Object customMarker;

	@NonNull
	@Singular
	ImmutableSet<IQueryOption> options;

	public static MeasurelessQueryBuilder edit(IWhereGroupByQuery step) {
		MeasurelessQueryBuilder builder =
				MeasurelessQuery.builder().filter(step.getFilter()).groupBy(step.getGroupBy());

		if (step instanceof IHasCustomMarker hasCustomMarker) {
			builder.customMarker(hasCustomMarker.getCustomMarker());
		}
		if (step instanceof IHasQueryOptions hasQueryOptions) {
			builder.options(hasQueryOptions.getOptions());
		}

		return builder;
	}
}