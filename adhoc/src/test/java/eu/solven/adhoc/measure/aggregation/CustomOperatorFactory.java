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
package eu.solven.adhoc.measure.aggregation;

import java.util.Map;

import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * This demonstrate how one can extends {@link StandardOperatorFactory}. We recommend composition over derivation.
 */
@Value
@Builder
public class CustomOperatorFactory implements IOperatorFactory {
	@Builder.Default
	@NonNull
	IOperatorFactory fallback = new StandardOperatorFactory();

	@Override
	public ICombination makeCombination(String key, Map<String, ?> options) {
		return switch (key) {
		case "CUSTOM": {
			yield new CustomCombination();
		}
		default:
			yield fallback.makeCombination(key, options);
		};
	}

	@Override
	public IAggregation makeAggregation(String key, Map<String, ?> options) {
		return switch (key) {
		case "CUSTOM": {
			yield new CustomAggregation();
		}
		default:
			yield fallback.makeAggregation(key, options);
		};
	}

	@Override
	public IDecomposition makeDecomposition(String key, Map<String, ?> decompositionOptions) {
		return switch (key) {
		case "CUSTOM": {
			yield new CustomDecomposition();
		}
		default:
			yield fallback.makeDecomposition(key, decompositionOptions);
		};
	}

	@Override
	public IFilterEditor makeEditor(String key, Map<String, ?> editorOptions) {
		return switch (key) {
		case "CUSTOM": {
			yield new CustomFilterEditor();
		}
		default:
			yield fallback.makeEditor(key, editorOptions);
		};
	}
}
