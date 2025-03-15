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
package eu.solven.adhoc.table.duckdb.quantile;

import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.record.ISlicedRecord;
import eu.solven.adhoc.slice.ISliceWithStep;
import eu.solven.adhoc.storage.IValueProvider;
import eu.solven.adhoc.storage.IValueReceiver;
import eu.solven.pepper.core.PepperLogHelper;

/**
 * Collecting the underlying measures into a single array.
 * 
 * @author Benoit Lacelle
 */
public class CustomArrayCombination implements ICombination {

	public static final String KEY = "ARRAY";

	@Override
	public IValueProvider combine(ISliceWithStep slice, ISlicedRecord slicedRecord) {
		int size = slicedRecord.size();

		// Storing into a int[] as it is easier to register unitTests
		int[] valuesAsArray = new int[size];

		for (int i = 0; i < size; i++) {
			int finalI = i;

			slicedRecord.read(i, new IValueReceiver() {

				@Override
				public void onLong(long v) {
					valuesAsArray[finalI] = (int) v;
				}

				@Override
				public void onDouble(double v) {
					valuesAsArray[finalI] = (int) v;
				}

				@Override
				public void onObject(Object v) {
					throw new IllegalArgumentException(
							"Unexpected type. Received %s".formatted(PepperLogHelper.getObjectAndClass(v)));
				}
			});
		}

		return vr -> vr.onObject(valuesAsArray);
	}

}
