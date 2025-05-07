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
package eu.solven.adhoc.measure.sum;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.cell.MultitypeCell;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.pepper.mappath.MapPathGet;

/**
 * A {@link ICombination} which multiplies the underlying measures. If any measure is null, the product is null.
 * 
 * @author Benoit Lacelle
 */
// https://learn.microsoft.com/en-us/dax/product-function-dax
public class ProductCombination implements ICombination {

	public static final String KEY = "PRODUCT";

	// If true, any null underlying leads to a null output
	// If false, null underlyings are ignored
	final boolean nullOperandIsNull;

	public ProductCombination() {
		nullOperandIsNull = true;
	}

	public ProductCombination(Map<String, ?> options) {
		nullOperandIsNull = MapPathGet.<Boolean>getOptionalAs(options, "nullOperandIsNull").orElse(true);
	}

	@Override
	public IValueProvider combine(ISliceWithStep slice, ISlicedRecord slicedRecord) {
		MultitypeCell refMultitype =
				MultitypeCell.builder().aggregation(new ProductAggregation()).asLong(1L).asDouble(1D).build();

		IValueReceiver cellValueConsumer = refMultitype.merge();
		AtomicBoolean hasNull = new AtomicBoolean();

		IValueReceiver proxyValueReceiver = new IValueReceiver() {

			@Override
			public void onLong(long v) {
				cellValueConsumer.onLong(v);
			}

			@Override
			public void onDouble(double v) {
				cellValueConsumer.onDouble(v);
			}

			@Override
			public void onObject(Object v) {
				if (v == null) {
					hasNull.set(true);
				} else {
					cellValueConsumer.onObject(v);
				}
			}
		};

		int size = slicedRecord.size();
		for (int i = 0; i < size; i++) {
			slicedRecord.read(i).acceptConsumer(proxyValueReceiver);
		}

		if (nullOperandIsNull && hasNull.get()) {
			return IValueProvider.NULL;
		}

		return refMultitype.reduce();
	}

}
