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
package eu.solven.adhoc.engine.tabular.splitter.merger;

import com.google.common.collect.LinkedHashMultimap;

import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.splitter.InduceByAdhoc;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.options.IHasQueryOptionsAndExecutorService;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * Will generate a minimal number of {@link TableQueryStep} while not merging any `GROUP BY`. Typically, `GROUP BY
 * (a,b)` and `GROUP BY (a,c)` is not merged into `GROUP BY (a,b,c)`.
 * 
 * However, filters can be merged as in `FILTER a1 GROUP BY (a,b)` and `GROUP BY (a)` can be merged into `GROUP BY
 * (a,b)`.
 * 
 * @author Benoit Lacelle
 */
@Deprecated(since = "Not-Ready. Need to merge the key steps")
@Slf4j
public class MergeInducersLaxGroupBy extends MergeInducersStrictGroupBy {

	@Builder(builderMethodName = "builderLax")
	MergeInducersLaxGroupBy(IFilterOptimizer filterOptimizer) {
		super(filterOptimizer);
	}

	@Override
	protected void mergeGroups(LinkedHashMultimap<TableQueryStep, TableQueryStep> mergingToSteps) {
		InduceByAdhoc inferrer = InduceByAdhoc.builder().build();
		IAdhocDag<TableQueryStep> dag =
				inferrer.splitInducedAsDag(IHasQueryOptionsAndExecutorService.noOption(), mergingToSteps.keySet());

		log.warn("TODO Dispatch dag={}", dag);

		super.mergeGroups(mergingToSteps);
	}

}
