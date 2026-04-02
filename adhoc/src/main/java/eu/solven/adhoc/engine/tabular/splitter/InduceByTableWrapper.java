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

import java.util.Set;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.GraphHelpers;
import eu.solven.adhoc.engine.tabular.optimizer.IAdhocDag;
import eu.solven.adhoc.options.IHasQueryOptionsAndExecutorService;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.adhoc.query.table.TableQueryV4;
import lombok.extern.slf4j.Slf4j;

/**
 * No {@link CubeQueryStep} is induced, leading to each of them needed to be computed by the {@link TableQueryV3}, hence
 * a large usage of `GROUPING SET`.
 * 
 * BEWARE This is quite inefficient for now, as we turn a {@link Set} of {@link TableQueryStep} into a
 * {@link TableQueryV4}, then a covering {@link TableQueryV3} which may produce a lot of junk (i.e. the covering v3
 * computes a lot of irrelevant steps).
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@Deprecated(since = "Not-Ready. May be useful once ITableWrapper supports UNION ALL")
public class InduceByTableWrapper implements ITableStepsSplitter {

	@Override
	public IAdhocDag<TableQueryStep> splitInducedAsDag(IHasQueryOptionsAndExecutorService hasOptions,
			IAdhocDag<TableQueryStep> inducedToInducer) {
		return GraphHelpers.makeGraph();
	}

	@Override
	public IAdhocDag<TableQueryStep> getLazyGraph(IHasQueryOptionsAndExecutorService hasOptions,
			IAdhocDag<TableQueryStep> inducedToInducer) {
		return GraphHelpers.makeGraph();
	}

}
