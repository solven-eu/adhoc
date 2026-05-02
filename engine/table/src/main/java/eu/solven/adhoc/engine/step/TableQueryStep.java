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
package eu.solven.adhoc.engine.step;

import java.util.concurrent.ConcurrentMap;

import org.jspecify.annotations.Nullable;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.options.IQueryOption;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.cube.IHasCustomMarker;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;

/**
 * A strongly-typed {@link ACubeQueryStep} whose measure is guaranteed to be an {@link Aggregator}.
 *
 * <p>
 * Used in the table-query execution path. Shares the {@code equals}/{@code hashCode} contract with
 * {@link CubeQueryStep} through {@link ACubeQueryStep}: a {@link TableQueryStep} and a {@link CubeQueryStep} carrying
 * the same aggregator, filter, groupBy, customMarker, and options are considered equal, enabling cross-type map lookups
 * without explicit casting.
 *
 * @author Benoit Lacelle
 * @see CubeQueryStep
 * @see ACubeQueryStep
 */
public final class TableQueryStep extends ACubeQueryStep {

	@NonNull
	private final Aggregator aggregator;

	@Builder
	protected TableQueryStep(ISliceFilter filter,
			IGroupBy groupBy,
			@Nullable Object customMarker,
			@Singular ImmutableSet<IQueryOption> options,
			ConcurrentMap<Object, Object> cache,
			@NonNull Aggregator aggregator) {
		super(filter, groupBy, customMarker, options, cache);
		this.aggregator = aggregator;
	}

	@Override
	public Aggregator getMeasure() {
		return aggregator;
	}

	/**
	 * Creates a {@link TableQueryStep} from a {@link CubeQueryStep} whose measure is an {@link Aggregator}.
	 *
	 * @throws IllegalArgumentException
	 *             if the step's measure is not an {@link Aggregator}
	 */
	public static TableQueryStep from(CubeQueryStep step) {
		if (!(step.getMeasure() instanceof Aggregator aggregator)) {
			throw new IllegalArgumentException(
					"Expected Aggregator but got: %s in %s".formatted(step.getMeasure(), step));
		}
		return TableQueryStep.builder()
				.filter(step.getFilter())
				.groupBy(step.getGroupBy())
				.customMarker(step.getCustomMarker())
				.options(step.getOptions())
				.cache(step.getCache())
				.aggregator(aggregator)
				.build();
	}

	/**
	 * Converts to an equivalent {@link CubeQueryStep}, preserving all fields. The returned step is equal to this one
	 * under the {@link ACubeQueryStep} equals contract.
	 */
	public CubeQueryStep toCubeQueryStep() {
		return CubeQueryStep.edit(this).cache(getCache()).build();
	}

	public static TableQueryStepBuilder edit(TableQueryStep step) {
		return TableQueryStep.builder()
				.aggregator(step.getMeasure())
				.customMarker(step.getCustomMarker())
				.filter(step.getFilter())
				.groupBy(step.getGroupBy())
				.options(step.getOptions());
	}

	public static TableQueryStepBuilder edit(IWhereGroupByQuery step) {
		TableQueryStepBuilder builder = TableQueryStep.builder().filter(step.getFilter()).groupBy(step.getGroupBy());

		if (step instanceof IHasCustomMarker hasCustomMarker) {
			hasCustomMarker.optCustomMarker().ifPresent(builder::customMarker);
		}
		if (step instanceof IHasQueryOptions hasQueryOptions) {
			builder.options(hasQueryOptions.getOptions());
		}
		if (step instanceof IHasMeasure hasMeasure) {
			builder.aggregator((@NonNull Aggregator) hasMeasure.getMeasure());
		}

		return builder;
	}

	public TableQueryStepBuilder toBuilder() {
		return edit(this);
	}
}
