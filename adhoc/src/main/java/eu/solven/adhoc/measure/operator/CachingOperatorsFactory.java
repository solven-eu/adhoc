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
package eu.solven.adhoc.measure.operator;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

/**
 * A decorating {@link IOperatorsFactory} with a cache-policy.
 * 
 * Typically used in early projects, where {@link IOperatorsFactory} generates a lot of Exceptions with a fall-back
 * strategy.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class CachingOperatorsFactory implements IOperatorsFactory {
	@Value
	@Builder
	static class OperatorsFactoryKey {
		@NonNull
		String key;
		@NonNull
		@Singular
		ImmutableMap<String, ?> options;
	}

	@NonNull
	IOperatorsFactory operatorsFactory;

	@NonNull
	@Default
	Cache<OperatorsFactoryKey, IAggregation> aggregationsCache = CacheBuilder.newBuilder().build();

	@NonNull
	@Default
	Cache<OperatorsFactoryKey, ICombination> combinationsCache = CacheBuilder.newBuilder().build();

	@NonNull
	@Default
	Cache<OperatorsFactoryKey, IDecomposition> decompositionsCache = CacheBuilder.newBuilder().build();
	@NonNull
	@Default
	Cache<OperatorsFactoryKey, IFilterEditor> filterEditorsCache = CacheBuilder.newBuilder().build();

	public void invalidateAll() {
		aggregationsCache.invalidateAll();
		combinationsCache.invalidateAll();
		decompositionsCache.invalidateAll();
		filterEditorsCache.invalidateAll();
	}

	@Override
	public IAggregation makeAggregation(String key, Map<String, ?> options) {
		OperatorsFactoryKey cacheKey = OperatorsFactoryKey.builder().key(key).options(options).build();
		try {
			return aggregationsCache.get(cacheKey, () -> operatorsFactory.makeAggregation(key, options));
		} catch (ExecutionException e) {
			throw new RuntimeException("Issue making aggregation for key=%s options=%s".formatted(key, options), e);
		}
	}

	@Override
	public ICombination makeCombination(String key, Map<String, ?> options) {
		OperatorsFactoryKey cacheKey = OperatorsFactoryKey.builder().key(key).options(options).build();
		try {
			return combinationsCache.get(cacheKey, () -> operatorsFactory.makeCombination(key, options));
		} catch (ExecutionException e) {
			throw new RuntimeException("Issue making combination for key=%s options=%s".formatted(key, options), e);
		}
	}

	@Override
	public IDecomposition makeDecomposition(String key, Map<String, ?> options) {
		OperatorsFactoryKey cacheKey = OperatorsFactoryKey.builder().key(key).options(options).build();
		try {
			return decompositionsCache.get(cacheKey, () -> operatorsFactory.makeDecomposition(key, options));
		} catch (ExecutionException e) {
			throw new RuntimeException("Issue making decomposition for key=%s options=%s".formatted(key, options), e);
		}
	}

	@Override
	public IFilterEditor makeEditor(String key, Map<String, ?> options) {
		OperatorsFactoryKey cacheKey = OperatorsFactoryKey.builder().key(key).options(options).build();
		try {
			return filterEditorsCache.get(cacheKey, () -> operatorsFactory.makeEditor(key, options));
		} catch (ExecutionException e) {
			throw new RuntimeException("Issue making aggregation for key=%s options=%s".formatted(key, options), e);
		}
	}

}
