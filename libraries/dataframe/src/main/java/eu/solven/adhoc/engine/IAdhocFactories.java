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
package eu.solven.adhoc.engine;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import eu.solven.adhoc.engine.measure.IMeasureQueryStepFactory;
import eu.solven.adhoc.filter.IFilterFactories;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.map.factory.ISliceFactoryFactory;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.options.IHasOptionsAndExecutorService;
import eu.solven.adhoc.util.IStopwatchFactory;

/**
 * Centralize the basic factories used through Adhoc.
 *
 * @author Benoit Lacelle
 */
public interface IAdhocFactories extends IFilterFactories {

	IOperatorFactory getOperatorFactory();

	IColumnFactory getColumnFactory();

	ISliceFactoryFactory getSliceFactoryFactory();

	default ISliceFactory getSliceFactory() {
		return getSliceFactoryFactory().makeFactory(IHasOptionsAndExecutorService.noOption());
	}

	IStopwatchFactory getStopwatchFactory();

	/**
	 * @return the executor service to use for parallel partition processing. Defaults to a direct (same-thread)
	 *         executor; override in production contexts (e.g. {@code AdhocFactoriesUnsafe}) to use
	 *         {@code AdhocUnsafe.adhocMixedPool}.
	 */
	default ListeningExecutorService getExecutorService() {
		return MoreExecutors.newDirectExecutorService();
	}

	IMeasureQueryStepFactory getMeasureQueryStepFactory();

}
