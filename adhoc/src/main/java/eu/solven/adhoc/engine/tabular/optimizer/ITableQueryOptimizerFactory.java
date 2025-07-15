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

import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.tabular.optimizer.ITableQueryOptimizer.SplitTableQueries;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.query.table.TableQuery;

/**
 * {@link ITableQueryOptimizerFactory} will turn an input {@link Set} of {@link TableQuery} into a
 * {@link SplitTableQueries}, telling which {@link TableQuery} will be executed and how to evaluate the other
 * {@link TableQuery} from the results of the later.
 * 
 * The inducer may or may not be amongst the provided input {@link TableQuery}.
 * 
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface ITableQueryOptimizerFactory {
	ITableQueryOptimizer makeOptimizer(AdhocFactories factories, IHasQueryOptions hasOptions);
}
