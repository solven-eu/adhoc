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

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.options.IHasQueryOptionsAndExecutorService;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.adhoc.table.ITableWrapper;

/**
 * {@link ITableQueryFactory} will turn an input {@link Set} of {@link TableQuery} into a {@link SplitTableQueries},
 * telling which {@link TableQuery} will be executed and how to evaluate the other {@link TableQuery} from the results
 * of the later.
 * 
 * The inducer may or may not be amongst the provided input {@link TableQuery}.
 * 
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface ITableQueryFactory {

	/**
	 * This will returns a {@link Set} of {@link TableQueryV3} to be execute by a {@link ITableWrapper}, and a DAG of
	 * {@link CubeQueryStep}. All leaves of the DAG should be evaluated by at least one {@link TableQueryV3}.
	 * 
	 * @param querySteps
	 *            the {@link CubeQueryStep} needed to be evaluated by the {@link ITableWrapper}
	 * @return an {@link SplitTableQueries} defining a {@link Set} of {@link TableQuery} from which all necessary
	 *         {@link TableQuery} can not be induced.
	 */
	SplitTableQueries splitInduced(IHasQueryOptionsAndExecutorService hasOptions, Set<TableQueryStep> querySteps);

	@Deprecated(since = ".splitInduced", forRemoval = true)
	default SplitTableQueries splitInducedLegacy(IHasQueryOptionsAndExecutorService hasOptions,
			Set<TableQuery> tableQueries) {
		Set<TableQueryStep> steps = tableQueries.stream().flatMap(tq -> {
			return tq.getAggregators().stream().map(agg -> TableQueryStep.edit(tq).aggregator(agg).build());
		}).collect(ImmutableSet.toImmutableSet());

		return splitInduced(hasOptions, steps);
	}

}
