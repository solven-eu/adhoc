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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.combination.FindFirstCombination;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * A decorating {@link IOperatorsFactory} with a cache-policy.
 * 
 * Typically used in early projects, where {@link IOperatorsFactory} generates a lot of Exceptions with a fall-back
 * strategy.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class CompositeOperatorsFactory implements IOperatorsFactory {
	@NonNull
	@Singular
	ImmutableList<IOperatorsFactory> operatorsFactories;

	@Override
	public IAggregation makeAggregation(String key, Map<String, ?> options) {
		return operatorsFactories.stream().map(of -> {
			try {
				return Optional.of(of.makeAggregation(key, options));
			} catch (RuntimeException e) {
				return Optional.<IAggregation>empty();
			}
		}).filter(Optional::isPresent).flatMap(Optional::stream).findFirst().orElseThrow(() -> new IllegalArgumentException("No aggregation for key={} optios={}".formatted(key, options)));
	}

	@Override
	public ICombination makeCombination(String key, Map<String, ?> options) {
		return operatorsFactories.stream().map(of -> {
			try {
				return Optional.of(of.makeCombination(key, options));
			} catch (RuntimeException e) {
				return Optional.<ICombination>empty();
			}
		}).filter(Optional::isPresent).flatMap(Optional::stream).findFirst().orElseThrow(() -> new IllegalArgumentException("No aggregation for key={} optios={}".formatted(key, options)));
	}

	@Override
	public IDecomposition makeDecomposition(String key, Map<String, ?> options) {
		return operatorsFactories.stream().map(of -> {
			try {
				return Optional.of(of.makeDecomposition(key, options));
			} catch (RuntimeException e) {
				return Optional.<IDecomposition>empty();
			}
		}).filter(Optional::isPresent).flatMap(Optional::stream).findFirst().orElseThrow(() -> new IllegalArgumentException("No aggregation for key={} optios={}".formatted(key, options)));
	}

	@Override
	public IFilterEditor makeEditor(String key, Map<String, ?> options) {
		return operatorsFactories.stream().map(of -> {
			try {
				return Optional.of(of.makeEditor(key, options));
			} catch (RuntimeException e) {
				return Optional.<IFilterEditor>empty();
			}
		}).filter(Optional::isPresent).flatMap(Optional::stream).findFirst().orElseThrow(() -> new IllegalArgumentException("No aggregation for key={} optios={}".formatted(key, options)));
	}

}
