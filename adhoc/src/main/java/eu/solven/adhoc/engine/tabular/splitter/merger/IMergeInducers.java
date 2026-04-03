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

import java.util.Set;

import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.filter.IFilterQueryBundle;

/**
 * Given a set of inducers, this will generate an additional set of inducers.
 * 
 * The point is typically to reduce the number of inducers.
 * 
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IMergeInducers {
	/**
	 * Creates a {@link IAdhocDag} to be merged into the main {@link IAdhocDag}. It will typically extends the leaves of
	 * the DAG, to merge inducers into a smaller number of inducers.
	 * 
	 * @param contextualAggregator
	 * @param steps
	 * @return
	 */
	IAdhocDag<TableQueryStep> mergeInducers(TableQueryStep contextualAggregator, Set<TableQueryStep> steps);

	/**
	 * Factory for {@link IMergeInducers}.
	 */
	@FunctionalInterface
	interface IMergeInducersFactory {
		/**
		 * Creates a new {@link IMergeInducers} using the query-scoped filter tools bundled in {@code filterBundle}.
		 *
		 * @param filterBundle
		 *            query-scoped stripper factory and cached optimizer
		 * @return a configured merger
		 */
		IMergeInducers makeMergeInducer(IFilterQueryBundle filterBundle);
	}
}
