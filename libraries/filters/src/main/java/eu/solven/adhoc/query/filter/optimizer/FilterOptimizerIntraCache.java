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
package eu.solven.adhoc.query.filter.optimizer;

import java.util.Collection;

import eu.solven.adhoc.query.filter.ISliceFilter;
import lombok.Builder.Default;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * An {@link IFilterOptimizer} which relies on a cache for given `.optimize`, but do not cache anything on the long-run.
 * It is especially relevant as the optimization are often recursive.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@SuperBuilder
public class FilterOptimizerIntraCache implements IFilterOptimizer {
	@Default
	final IOptimizerEventListener listener = new IOptimizerEventListener() {

	};

	protected FilterOptimizerWithCache makeWithCache() {
		return FilterOptimizerWithCache.builder().listener(listener).build();
	}

	@Override
	public ISliceFilter and(Collection<? extends ISliceFilter> filters, boolean willBeNegated) {
		return makeWithCache().and(filters, willBeNegated);
	}

	@Override
	public ISliceFilter or(Collection<? extends ISliceFilter> filters) {
		return makeWithCache().or(filters);
	}

	@Override
	public ISliceFilter not(ISliceFilter filter) {
		return makeWithCache().not(filter);
	}
}
