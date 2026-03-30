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
package eu.solven.adhoc.engine.tabular.splitter;

import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.IGroupBy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Holds generic behavior to {@link ITableStepsSplitter} which leads to steps being induced by Adhoc.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@RequiredArgsConstructor
public abstract class AInduceByAdhocParent implements ITableStepsSplitter {

	protected TableQueryStep contextOnly(TableQueryStep inducer) {
		return TableQueryStep.edit(inducer)
				.aggregator(Aggregator.sum("noMeasure"))
				.groupBy(IGroupBy.GRAND_TOTAL)
				.filter(ISliceFilter.MATCH_ALL)
				.build();
	}

}
