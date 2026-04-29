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

import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.table.TableQueryV3;

/**
 * This {@link ITableStepsGrouper} will make one {@link TableQueryV3} per considered {@link Aggregator}.
 * 
 * This strategy will generate a limited number of {@link TableQueryV3} per {@link CubeQuery}: one per requested
 * aggregation (i.e. column+aggregationKey).
 * 
 * @author Benoit Lacelle
 */
public class TableStepsGrouperByAggregator extends TableStepsGrouper {

	/**
	 * We want a single tableQuery per measure. May be useful to improve table performance, concurrency and readability
	 * (by having 1 SQL per aggregated measure).
	 * 
	 * @param inducer
	 * @return
	 */
	@Override
	public TableQueryStep tableQueryGroupBy(TableQueryStep inducer) {
		return contextOnly(inducer).toBuilder().aggregator(inducer.getMeasure()).build();
	}
}
