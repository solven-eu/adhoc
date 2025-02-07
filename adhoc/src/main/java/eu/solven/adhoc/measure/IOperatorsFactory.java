/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.measure;

import java.util.Map;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.step.Combinator;
import eu.solven.adhoc.measure.step.ICombineUnderlyingMeasures;

/**
 * Provides {@link ICombination} given their key. This can be extended to provides custom transformations.
 *
 * @author Benoit Lacelle
 */
public interface IOperatorsFactory {

	default ICombination makeCombination(ICombineUnderlyingMeasures hasCombinationKey) {
		Map<String, ?> allOptions =
				Combinator.makeAllOptions(hasCombinationKey, hasCombinationKey.getCombinationOptions());
		return makeCombination(hasCombinationKey.getCombinationKey(), allOptions);
	}

	ICombination makeCombination(String key, Map<String, ?> options);

	/**
	 * @param key
	 * @param options
	 * @return an instance of IAggregation matching given key
	 */
	IAggregation makeAggregation(String key, Map<String, ?> options);

	/**
	 * Use empty options by default, as most IAggregation are not configurable.
	 *
	 * @param key
	 * @return an instance of IAggregation matching given key
	 */
	default IAggregation makeAggregation(String key) {
		return makeAggregation(key, Map.of());
	}

	IDecomposition makeDecomposition(String key, Map<String, ?> decompositionOptions);
}
