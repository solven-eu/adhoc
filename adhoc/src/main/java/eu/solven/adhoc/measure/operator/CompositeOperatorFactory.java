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
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

/**
 * A decorating {@link IOperatorFactory} with a cache-policy.
 * 
 * Typically used in early projects, where {@link IOperatorFactory} generates a lot of Exceptions with a fall-back
 * strategy.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class CompositeOperatorFactory implements IOperatorFactory {
	@NonNull
	@Singular
	ImmutableList<IOperatorFactory> operatorFactories;

	protected void logOnException(String method, String key, Map<String, ?> options, IOperatorFactory of, Throwable t) {
		if (log.isDebugEnabled()) {
			log.warn("Issue with {} with key={} options={} on operatorFactory={}", method, key, options, of, t);
		} else {
			log.warn("Issue with {} with key={} options={} on operatorFactory={} e={}",
					method,
					key,
					options,
					of,
					t.getMessage());
		}
	}

	@Override
	public IAggregation makeAggregation(String key, Map<String, ?> options) {
		return operatorFactories.stream().map(of -> {
			try {
				return Optional.of(of.makeAggregation(key, options));
			} catch (RuntimeException e) {
				logOnException(".makeAggregation", key, options, of, e);
				return Optional.<IAggregation>empty();
			}
		})
				.filter(Optional::isPresent)
				.flatMap(Optional::stream)
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException(
						"No aggregation for key=%s options=%s".formatted(key, options)));
	}

	@Override
	public ICombination makeCombination(String key, Map<String, ?> options) {
		return operatorFactories.stream().map(of -> {
			try {
				return Optional.of(of.withRoot(this).makeCombination(key, options));
			} catch (RuntimeException e) {
				logOnException(".makeCombination", key, options, of, e);
				return Optional.<ICombination>empty();
			}
		})
				.filter(Optional::isPresent)
				.flatMap(Optional::stream)
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException(
						"No combination for key=%s options=%s".formatted(key, options)));
	}

	@Override
	public IDecomposition makeDecomposition(String key, Map<String, ?> options) {
		return operatorFactories.stream().map(of -> {
			try {
				return Optional.of(of.makeDecomposition(key, options));
			} catch (RuntimeException e) {
				logOnException(".makeDecomposition", key, options, of, e);
				return Optional.<IDecomposition>empty();
			}
		})
				.filter(Optional::isPresent)
				.flatMap(Optional::stream)
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException(
						"No decomposition for key=%s options=%s".formatted(key, options)));
	}

	@Override
	public IFilterEditor makeEditor(String key, Map<String, ?> options) {
		return operatorFactories.stream().map(of -> {
			try {
				return Optional.of(of.makeEditor(key, options));
			} catch (RuntimeException e) {
				logOnException(".makeEditor", key, options, of, e);
				return Optional.<IFilterEditor>empty();
			}
		})
				.filter(Optional::isPresent)
				.flatMap(Optional::stream)
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException(
						"No filterEditor for key=%s options=%s".formatted(key, options)));
	}

	@Override
	public IOperatorFactory withRoot(IOperatorFactory rootOperatorFactory) {
		return CompositeOperatorFactory.builder()
				.operatorFactories(operatorFactories.stream().map(of -> of.withRoot(rootOperatorFactory)).toList())
				.build();
	}

}
