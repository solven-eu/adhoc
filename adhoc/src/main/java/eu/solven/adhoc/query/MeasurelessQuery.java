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

import eu.solven.adhoc.dag.step.AdhocQueryStep;
import eu.solven.adhoc.debug.IIsDebugable;
import eu.solven.adhoc.debug.IIsExplainable;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IHasCustomMarker;
import eu.solven.adhoc.query.cube.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Typically used to group aggregators together, given they are associated to the same filter, groupBy, etc clauses.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
public class MeasurelessQuery implements IWhereGroupbyAdhocQuery, IHasCustomMarker, IIsExplainable, IIsDebugable {

	@NonNull
	IAdhocFilter filter;

	@NonNull
	IAdhocGroupBy groupBy;

	Object customMarker;

	// This is part of hashcodeEquals
	// It means we may have a different queryPlan when a subset of querySteps are debuggable
	@Builder.Default
	boolean explain = false;

	// This is part of hashcodeEquals
	// It means we may have a different queryPlan when a subset of querySteps are debuggable
	@Builder.Default
	boolean debug = false;

	public static MeasurelessQueryBuilder edit(AdhocQueryStep step) {
		return MeasurelessQuery.builder()
				.filter(step.getFilter())
				.groupBy(step.getGroupBy())
				.customMarker(step.getCustomMarker())
				.explain(step.isExplain())
				.debug(step.isDebug());
	}
}