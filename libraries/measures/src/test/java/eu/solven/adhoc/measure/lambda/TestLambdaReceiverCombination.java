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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.data.cell.ProxyValueReceiver;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.data.row.SlicedRecordFromArray;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;

public class TestLambdaReceiverCombination {
	ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

	@Test
	public void testCombine_plus1AsLong() {
		LambdaReceiverCombination combination =
				new LambdaReceiverCombination(Map.of(LambdaReceiverCombination.K_LAMBDA,
						(LambdaReceiverCombination.ILambdaReceiverCombination) (s,
								slicedRecord,
								receiver) -> slicedRecord.read(0, new IValueReceiver() {
									@Override
									public void onLong(long v) {
										receiver.onLong(v + 1L);
									}

									@Override
									public void onObject(Object v) {
										if (v == null) {
											receiver.onObject(null);
										} else {
											receiver.onLong(((Number) v).longValue() + 1L);
										}
									}
								})));

		ISlicedRecord record = SlicedRecordFromArray.builder().measure(41L).build();
		ProxyValueReceiver out = ProxyValueReceiver.builder().build();
		combination.combine(slice, record, out);

		Assertions.assertThat(IValueProvider.getValue(out.asValueProvider())).isEqualTo(42L);
	}

	@Test
	public void testCombine_listFallback_routesThroughReceiverPath() {
		// The List<?> default in ICombination delegates to the receiver shape, so the lambda is still invoked.
		LambdaReceiverCombination combination = new LambdaReceiverCombination(Map.of(LambdaReceiverCombination.K_LAMBDA,
				(LambdaReceiverCombination.ILambdaReceiverCombination) (s, slicedRecord, receiver) -> receiver
						.onObject("output")));

		Assertions.assertThat(combination.combine(slice, java.util.List.of(1L))).isEqualTo("output");
	}

	@Test
	public void testCombine_usesSlice() {
		LambdaReceiverCombination combination = new LambdaReceiverCombination(Map.of(LambdaReceiverCombination.K_LAMBDA,
				(LambdaReceiverCombination.ILambdaReceiverCombination) (s, slicedRecord, receiver) -> receiver
						.onObject(s)));

		ISlicedRecord record = SlicedRecordFromArray.builder().measure(1L).build();
		ProxyValueReceiver out = ProxyValueReceiver.builder().build();
		combination.combine(slice, record, out);

		Assertions.assertThat(IValueProvider.getValue(out.asValueProvider())).isSameAs(slice);
	}

	@Test
	public void testConstruct_missingLambda() {
		Assertions.assertThatThrownBy(() -> new LambdaReceiverCombination(Map.of()))
				.isInstanceOf(IllegalArgumentException.class);
	}
}
