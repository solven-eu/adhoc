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

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.api.v1.IHasCustomMarker;
import eu.solven.adhoc.api.v1.IIsDebugable;
import eu.solven.adhoc.api.v1.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.transformers.IMeasure;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
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
@EqualsAndHashCode(exclude = "debug")
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
	Object customMarker;

	public static AdhocQueryStepBuilder edit(AdhocQueryStep step) {
		return edit((IWhereGroupbyAdhocQuery) step).measure(step.getMeasure());
	}

	public static AdhocQueryStepBuilder edit(IWhereGroupbyAdhocQuery step) {
		AdhocQueryStepBuilder builder = AdhocQueryStep.builder().filter(step.getFilter()).groupBy(step.getGroupBy());

		if (step instanceof IIsDebugable debugable) {
			builder.debug(debugable.isDebug());
		}

		return builder;
	}

}
