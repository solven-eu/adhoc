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
package eu.solven.adhoc.dag;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import eu.solven.adhoc.debug.IIsDebugable;
import eu.solven.adhoc.measure.AdhocMeasureBag;
import eu.solven.adhoc.measure.IMeasure;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.query.cube.IHasCustomMarker;
import eu.solven.adhoc.query.cube.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import lombok.Value;

/**
 * Given an {@link IAdhocQuery} and a {@link AdhocMeasureBag}, we need to compute each underlying measure at a given
 * {@link IWhereGroupbyAdhocQuery}.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@EqualsAndHashCode(exclude = {
		// being debug or not should not prevent 2 querySteps to be considered equals in some hashStructure
		// This could lead to unexpected debug/notDebug
		"debug",
		// cache is typically used for performance improvments: it should not impact any hashStructure
		"cache" })
@ToString(exclude = {
		// The cache is not relevant in logs. This may be tweaked based on `debug` flag
		"cache" })
public class AdhocQueryStep implements IWhereGroupbyAdhocQuery, IIsDebugable, IHasCustomMarker {
	@NonNull
	IMeasure measure;
	@NonNull
	IAdhocFilter filter;
	@NonNull
	IAdhocGroupBy groupBy;

	@Default
	boolean debug = false;

	// This property is transported down to the DatabaseQuery
	@Default
	Object customMarker = null;

	// Used to store transient information, like slow-to-evaluate information
	Map<Object, Object> cache = new ConcurrentHashMap<>();

	public static AdhocQueryStepBuilder edit(AdhocQueryStep step) {
		return edit((IWhereGroupbyAdhocQuery) step).measure(step.getMeasure());
	}

	public static AdhocQueryStepBuilder edit(IWhereGroupbyAdhocQuery step) {
		AdhocQueryStepBuilder builder = AdhocQueryStep.builder().filter(step.getFilter()).groupBy(step.getGroupBy());

		if (step instanceof IIsDebugable debuggable) {
			builder.debug(debuggable.isDebug());
		}
		if (step instanceof IHasCustomMarker hasCustomMarker) {
			hasCustomMarker.optCustomMarker().ifPresent(builder::customMarker);
		}

		return builder;
	}

}
