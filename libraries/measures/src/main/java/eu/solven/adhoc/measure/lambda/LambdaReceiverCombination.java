/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.measure.lambda;

import java.util.Map;

import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.util.map.AdhocMapPathGet;

/**
 * Variant of {@link LambdaCombination} that exposes the lower-level
 * {@link ICombination#combine(ISliceWithStep, ISlicedRecord, IValueReceiver)} entry point. The lambda is handed the
 * {@link ISlicedRecord} and the output {@link IValueReceiver} directly, so it can read underlying values via
 * {@link ISlicedRecord#read(int, IValueReceiver)} and write the result via {@link IValueReceiver#onLong(long)} or
 * {@link IValueReceiver#onDouble(double)} without going through the boxed `List<?>` path. Useful when allocation cost
 * on the per-cell path matters.
 *
 * @author Benoit Lacelle
 */
public class LambdaReceiverCombination implements ICombination {
	public static final String K_LAMBDA = "lambda";

	final ILambdaReceiverCombination lambda;

	/**
	 * Functional shape mirroring {@link ICombination#combine(ISliceWithStep, ISlicedRecord, IValueReceiver)}. Beware
	 * this is typically not serializable.
	 */
	@FunctionalInterface
	public interface ILambdaReceiverCombination {
		void combine(ISliceWithStep slice, ISlicedRecord slicedRecord, IValueReceiver receiver);
	}

	public LambdaReceiverCombination(Map<String, ?> options) {
		lambda = AdhocMapPathGet.getRequiredAs(options, K_LAMBDA);
	}

	@Override
	public void combine(ISliceWithStep slice, ISlicedRecord slicedRecord, IValueReceiver receiver) {
		lambda.combine(slice, slicedRecord, receiver);
	}
}
