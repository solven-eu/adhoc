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

import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.util.AdhocIdentity;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * An {@link IOperatorFactory} which always return the same dummy. Useful for early projects with a predefined
 * {@link eu.solven.adhoc.measure.IMeasureForest}.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class DummyOperatorFactory implements IOperatorFactory {
	@NonNull
	@Builder.Default
	IAggregation dummyAggregation = new SumAggregation();
	@NonNull
	@Builder.Default
	ICombination dummyCombination = new CoalesceCombination();
	@NonNull
	@Builder.Default
	IDecomposition dummyDecomposition = new AdhocIdentity();
	@NonNull
	@Builder.Default
	IFilterEditor dummyEditor = new AdhocIdentity();

	@Override
	public IAggregation makeAggregation(String key, Map<String, ?> options) {
		return dummyAggregation;
	}

	@Override
	public ICombination makeCombination(String key, Map<String, ?> options) {
		return dummyCombination;
	}

	@Override
	public IDecomposition makeDecomposition(String key, Map<String, ?> options) {
		return dummyDecomposition;
	}

	@Override
	public IFilterEditor makeEditor(String key, Map<String, ?> options) {
		return dummyEditor;
	}

	@Override
	public IOperatorFactory withRoot(IOperatorFactory rootOperatorFactory) {
		return this;
	}

}
