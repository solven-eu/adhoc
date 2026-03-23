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
package eu.solven.adhoc.engine.tabular.grouper;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.options.IQueryOption;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.table.TableQueryV3;

/**
 * This {@link ITableStepsGrouper} will generate a {@link TableQueryV3} by differing options.
 *
 * Custom markers are nullified (but this may can be easily tweaked): if 2 {@link CubeQueryStep} differs by
 * customMarker, they will be executed a single {@link TableQueryV3} with no customMarker.
 *
 * This {@link ITableStepsGrouper} will minimize the number of {@link TableQueryV3} per {@link CubeQuery}.
 *
 * @author Benoit Lacelle
 */
public class TableStepsGrouper implements ITableStepsGrouper {

	@Override
	public TableQueryStep tableQueryGroupBy(TableQueryStep inducer) {
		return contextOnly(inducer);
	}

	/**
	 * Check everything representing the context of the query. Typically represents the {@link IQueryOption} and the
	 * customMarker.
	 *
	 * @param inducer
	 * @return a CubeQueryStep which has been fleshed-out of what's not the query context.
	 */
	protected TableQueryStep contextOnly(TableQueryStep inducer) {
		return inducer.toBuilder()
				.aggregator(Aggregator.sum("noMeasure"))
				.groupBy(IGroupBy.GRAND_TOTAL)
				.filter(ISliceFilter.MATCH_ALL)
				.customMarker(contextOnly(inducer.getCustomMarker()))
				.build();
	}

	// @SuppressWarnings("PMD.EmptyMethodInAbstractClassShouldBeAbstract")
	protected Object contextOnly(Object customMarker) {
		// In most cases, 2 different customMarker should lead to a single tableQuery, except in very special scenario
		// (Example: `CCY=EUR` and `CCY=USD` may map to 2 different tables).
		// TODO UnitTest this scenario

		// TODO For now we return the customMarker, as we did not find how to manage null properly
		// Typically, we need a way to restore customMarker in
		// ITableQueryOptimizer.SplitTableQueries.forEachCubeQuerySteps
		return customMarker;
	}

}
