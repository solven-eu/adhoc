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
package eu.solven.adhoc.aggregations;

import java.util.Map;

import eu.solven.adhoc.aggregations.max.MaxAggregator;
import eu.solven.adhoc.aggregations.max.MaxTransformation;
import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.aggregations.sum.SumCombination;

public class StandardOperatorsFactory implements IOperatorsFactory {

	@Override
	public ICombination makeCombination(String key, Map<String, ?> options) {
		return switch (key) {
		case SumCombination.KEY: {
			yield new SumCombination();
		}
		case MaxTransformation.KEY: {
			yield new MaxTransformation();
		}
		case DivideCombination.KEY: {
			yield new DivideCombination();
		}
		case ExpressionCombination.KEY: {
			yield ExpressionCombination.parse(options);
		}
		default:
			throw new IllegalArgumentException("Unexpected value: " + key);
		};
	}

	@Override
	public IAggregation makeAggregation(String key) {
		return switch (key) {
		case SumAggregator.KEY: {
			yield new SumAggregator();
		}
		case MaxAggregator.KEY: {
			yield new MaxAggregator();
		}
		default:
			throw new IllegalArgumentException("Unexpected value: " + key);
		};
	}

	@Override
	public IDecomposition makeDecomposition(String key, Map<String, ?> options) {
		return switch (key) {
		case AdhocIdentity.KEY: {
			yield new AdhocIdentity();
		}
		case LinearDecomposition.KEY: {
			yield new LinearDecomposition(options);
		}
		default:
			throw new IllegalArgumentException("Unexpected value: " + key);
		};
	}

}
