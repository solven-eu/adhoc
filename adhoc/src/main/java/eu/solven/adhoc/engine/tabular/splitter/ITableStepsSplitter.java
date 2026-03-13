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

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.table.ITableWrapper;

/**
 * Given a set of {@link CubeQueryStep}, defines a DAG of steps induced by Adhoc. The leaves of this DAG has to be
 * evaluated by the {@link ITableWrapper}.
 * 
 * Typically, ITableWrapper supporting `FILTER` and/or `GROUPING SET` may cover more leaves at lower cost, reducing the
 * work left to Adhoc.
 * 
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface ITableStepsSplitter {

	DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> splitInducedAsDag(IHasQueryOptions hasOptions,
			Set<CubeQueryStep> tableSteps);

}
