/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.engine.tabular.optimizer;

import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.splitter.InduceByGroupingSets;
import eu.solven.adhoc.engine.tabular.splitter.TableStepsGrouperNoGroup;
import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;
import lombok.extern.slf4j.Slf4j;

/**
 * Force a single SQL per {@link CubeQueryStep}. May be useful for very simple DAG, or for debug purposes.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class TableQueryOptimizerSinglePerCubeStep extends TableQueryOptimizer {

	public TableQueryOptimizerSinglePerCubeStep(IAdhocFactories factories, IFilterOptimizer filterOptimizer) {
		super(factories, filterOptimizer, new InduceByGroupingSets(), new TableStepsGrouperNoGroup());
	}

}
