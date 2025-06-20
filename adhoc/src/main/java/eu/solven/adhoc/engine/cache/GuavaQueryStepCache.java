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
package eu.solven.adhoc.engine.cache;

import java.util.Map;
import java.util.Optional;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import lombok.Builder;
import lombok.NonNull;

/**
 * A {@link IQueryStepCache} based on Guava {@link Cache}.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class GuavaQueryStepCache implements IQueryStepCache {
	@NonNull
	Cache<CubeQueryStep, ISliceToValue> queryStepToValues;

	static Cache<CubeQueryStep, ISliceToValue> makeCache(long size) {
		return CacheBuilder.newBuilder()
				.maximumWeight(size)
				// TODO We'd like to keep in priority columns which were slow to compute (by table or by transformator)
				.<CubeQueryStep, ISliceToValue>weigher((queryStep, sliceToValue) -> {
					return Ints.checkedCast(sliceToValue.size());
				})
				.recordStats()
				.build();
	}

	@Override
	public Optional<ISliceToValue> getValue(CubeQueryStep step) {
		return Optional.ofNullable(queryStepToValues.getIfPresent(step));
	}

	public static GuavaQueryStepCache withSize(long cacheSize) {
		return GuavaQueryStepCache.builder().queryStepToValues(makeCache(cacheSize)).build();
	}

	@Override
	public void pushValues(Map<CubeQueryStep, ISliceToValue> queryStepToValues) {
		this.queryStepToValues.putAll(queryStepToValues);
	}
}
