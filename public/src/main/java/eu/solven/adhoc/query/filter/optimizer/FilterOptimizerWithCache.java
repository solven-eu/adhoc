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
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.util.IHasCache;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Extends {@link FilterOptimizer} to enable caching. The caching shall catch any intermediate {@link ISliceFilter}
 * being optimized.
 * 
 * The default caching strategy has no invalidation: as it may memory-leak, it should not be used for long-running
 * processed. However, it can be used in the context of a single query.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@SuperBuilder
public class FilterOptimizerWithCache extends FilterOptimizer implements IHasCache {
	final Cache<Set<ISliceFilter>, ISliceFilter> optimizedAndNegated = CacheBuilder.newBuilder().build();
	final Cache<Set<ISliceFilter>, ISliceFilter> optimizedAndNotNegated = CacheBuilder.newBuilder().build();

	final Cache<Set<ISliceFilter>, ISliceFilter> optimizedOrs = CacheBuilder.newBuilder().build();
	final Cache<ISliceFilter, ISliceFilter> optimizedNot = CacheBuilder.newBuilder().build();

	@Override
	public void invalidateAll() {
		optimizedAndNegated.invalidateAll();
		optimizedAndNotNegated.invalidateAll();
		optimizedOrs.invalidateAll();
		optimizedNot.invalidateAll();
	}

	@Override
	public ISliceFilter and(Collection<? extends ISliceFilter> filters, boolean willBeNegated) {
		Cache<Set<ISliceFilter>, ISliceFilter> cache;
		if (willBeNegated) {
			cache = optimizedAndNegated;
		} else {
			cache = optimizedAndNotNegated;
		}

		try {
			return cache.get(ImmutableSet.copyOf(filters), () -> {
				return notCachedAnd(filters, willBeNegated);
			});
		} catch (ExecutionException e) {
			throw new IllegalArgumentException("Issue on AND over %s".formatted(filters), e);
		}
	}

	@Override
	public ISliceFilter or(Collection<? extends ISliceFilter> filters) {
		try {
			return optimizedOrs.get(ImmutableSet.copyOf(filters), () -> {
				return notCachedOr(filters);
			});
		} catch (ExecutionException e) {
			throw new IllegalArgumentException("Issue on OR over %s".formatted(filters), e);
		}
	}

	@Override
	public ISliceFilter not(ISliceFilter filter) {
		try {
			return optimizedNot.get(filter, () -> {
				return notCachedNot(filter);
			});
		} catch (ExecutionException e) {
			throw new IllegalArgumentException("Issue on NOT over %s".formatted(filter), e);
		}
	}

}
