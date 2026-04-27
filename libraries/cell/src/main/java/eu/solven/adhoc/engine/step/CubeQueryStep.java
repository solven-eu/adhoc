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
package eu.solven.adhoc.engine.step;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jspecify.annotations.Nullable;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.options.IQueryOption;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.cube.IHasCustomMarker;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;

/**
 * Given an {@link ICubeQuery} and a {@link MeasureForest}, we need to compute each underlying measure at a given
 * {@link IWhereGroupByQuery}.
 *
 * @author Benoit Lacelle
 */
public final class CubeQueryStep extends ACubeQueryStep {

	@NonNull
	@Getter
	final IMeasure measure;

	@Builder
	protected CubeQueryStep(ISliceFilter filter,
			IGroupBy groupBy,
			@Nullable Object customMarker,
			@Singular ImmutableSet<IQueryOption> options,
			ConcurrentMap<Object, Object> cache,
			@NonNull IMeasure measure) {
		super(filter, groupBy, customMarker, options, cache);
		this.measure = measure;
	}

	/**
	 * Returns a builder pre-populated with all fields of this step (cache is reset, preserving the transverse cache).
	 */
	public CubeQueryStepBuilder toBuilder() {
		ConcurrentMap<Object, Object> newCache = new ConcurrentHashMap<>();
		if (getCache().containsKey(KEY_CACHE_TRANSVERSE)) {
			newCache.put(KEY_CACHE_TRANSVERSE, getTransverseCache());
		}
		return CubeQueryStep.builder()
				.filter(getFilter())
				.groupBy(getGroupBy())
				.customMarker(getCustomMarker())
				.options(getOptions())
				.cache(newCache)
				.measure(getMeasure());
	}

	public static CubeQueryStepBuilder edit(IWhereGroupByQuery step) {
		CubeQueryStepBuilder builder = CubeQueryStep.builder().filter(step.getFilter()).groupBy(step.getGroupBy());

		if (step instanceof IHasCustomMarker hasCustomMarker) {
			hasCustomMarker.optCustomMarker().ifPresent(builder::customMarker);
		}
		if (step instanceof IHasQueryOptions hasQueryOptions) {
			builder.options(hasQueryOptions.getOptions());
		}
		if (step instanceof IHasMeasure hasMeasure) {
			builder.measure(hasMeasure.getMeasure());
		}

		return builder;
	}

	/**
	 * Lombok @Builder — extends with a {@code measure(String)} convenience overload. Both setter variants must be
	 * declared here because Lombok skips generating any setter whose name is already present in the custom class.
	 */
	public static class CubeQueryStepBuilder {
		@SuppressWarnings({ "PMD.AvoidFieldNameMatchingMethodName", "PMD.UnusedPrivateField" })
		private IMeasure measure;

		public CubeQueryStepBuilder measure(IMeasure measure) {
			this.measure = measure;
			return this;
		}

		public CubeQueryStepBuilder measure(String measureName) {
			return measure(ReferencedMeasure.ref(measureName));
		}
	}
}
