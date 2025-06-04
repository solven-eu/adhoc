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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.debug.IIsDebugable;
import eu.solven.adhoc.debug.IIsExplainable;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.model.IHasTags;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.cube.IHasCustomMarker;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Singular;
import lombok.ToString;
import lombok.Value;

/**
 * Given an {@link ICubeQuery} and a {@link MeasureForest}, we need to compute each underlying measure at a given
 * {@link IWhereGroupByQuery}.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@EqualsAndHashCode(exclude = {
		// cache is typically used for performance improvements: it should not impact any hashStructure
		"cache" })
@ToString(exclude = {
		// The cache is not relevant in logs. This may be tweaked based on `debug` flag
		"cache" })
// BEWARE Should we have a ref to the IAdhocCubeBuilder, which may be useful for instance in ICombination of some
// measure
public class CubeQueryStep implements IWhereGroupByQuery, IHasCustomMarker, IHasQueryOptions {
	@NonNull
	IMeasure measure;
	@NonNull
	@Default
	IAdhocFilter filter = IAdhocFilter.MATCH_ALL;
	@NonNull
	@Default
	IAdhocGroupBy groupBy = IAdhocGroupBy.GRAND_TOTAL;

	// This property is transported down to the DatabaseQuery
	@Default
	Object customMarker = null;

	@NonNull
	@Singular
	ImmutableSet<IQueryOption> options;

	// Used to store transient information, like slow-to-evaluate information
	Map<Object, Object> cache = new ConcurrentHashMap<>();

	/**
	 * Lombok @Builder
	 * 
	 * @author Benoit Lacelle
	 */
	public static class CubeQueryStepBuilder {
		@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
		IMeasure measure;

		public CubeQueryStepBuilder measure(IMeasure measure) {
			this.measure = measure;
			return this;
		}

		public CubeQueryStepBuilder measure(String measureName) {
			return measure(ReferencedMeasure.ref(measureName));
		}
	}

	public static CubeQueryStepBuilder edit(CubeQueryStep step) {
		return edit((IWhereGroupByQuery) step).measure(step.getMeasure());
	}

	public static CubeQueryStepBuilder edit(IWhereGroupByQuery step) {
		CubeQueryStepBuilder builder = CubeQueryStep.builder().filter(step.getFilter()).groupBy(step.getGroupBy());

		if (step instanceof IHasCustomMarker hasCustomMarker) {
			hasCustomMarker.optCustomMarker().ifPresent(builder::customMarker);
		}
		if (step instanceof IHasQueryOptions hasQueryOptions) {
			builder.options(hasQueryOptions.getOptions());
		} else {
			if (step instanceof IIsExplainable explainable && explainable.isExplain()) {
				builder.option(StandardQueryOptions.EXPLAIN);
			}
			if (step instanceof IIsDebugable debuggable && debuggable.isDebug()) {
				builder.option(StandardQueryOptions.DEBUG);
			}
		}

		return builder;
	}

	@Override
	public boolean isDebug() {
		return getOptions().contains(StandardQueryOptions.DEBUG) || measure.getTags().contains(IHasTags.TAG_DEBUG);
	}

}
