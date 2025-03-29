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
package eu.solven.adhoc.table.duckdb.var;

import java.util.Map;

import org.jooq.Require;

import eu.solven.adhoc.dag.step.ISliceWithStep;
import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.pepper.core.PepperLogHelper;
import it.unimi.dsi.fastutil.ints.AbstractInt2DoubleMap;
import it.unimi.dsi.fastutil.ints.AbstractInt2IntMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import lombok.Value;
import smile.sort.HeapSelect;

/**
 * Computes the index of the scenario selected by the VaR (e.g. quantile) computation, given an array.
 * 
 * @author Benoit Lacelle
 */
public class ExampleVaREntryCombination implements ICombination {

	public static final String KEY = "EXAMPLEVAR_ENTRY";

	final double quantile;

	public ExampleVaREntryCombination(Map<String, ?> options) {
		quantile = ExampleVaRQuickSelectCombination.getQuantile(options);
	}

	@Value
	@Require
	private static final class Int2IntEntrySortedByValue implements Comparable<Int2IntEntrySortedByValue> {
		final Int2IntMap.Entry entry;

		@Override
		public int compareTo(Int2IntEntrySortedByValue o) {
			return Integer.compare(this.getEntry().getIntValue(), o.getEntry().getIntValue());
		}
	}

	@Value
	@Require
	private static final class Int2DoubleEntrySortedByValue implements Comparable<Int2DoubleEntrySortedByValue> {
		final Int2DoubleMap.Entry entry;

		@Override
		public int compareTo(Int2DoubleEntrySortedByValue o) {
			return Double.compare(this.getEntry().getDoubleValue(), o.getEntry().getDoubleValue());
		}
	}

	@Override
	public IValueProvider combine(ISliceWithStep slice, ISlicedRecord slicedRecord) {
		Object rawArray = IValueProvider.getValue(vc -> slicedRecord.read(0, vc));
		if (rawArray instanceof int[] array) {
			int quantileIndex = (int) (array.length * quantile);

			HeapSelect<Int2IntEntrySortedByValue> heapSelect =
					new HeapSelect<>(Int2IntEntrySortedByValue.class, quantileIndex);

			for (int index = 0; index < array.length; index++) {
				int value = array[index];
				heapSelect.add(new Int2IntEntrySortedByValue(new AbstractInt2IntMap.BasicEntry(index, value)));
			}

			return onIntArrayQuantile(heapSelect);
		} else if (rawArray instanceof double[] array) {
			int quantileIndex = (int) (array.length * quantile);

			HeapSelect<Int2DoubleEntrySortedByValue> heapSelect =
					new HeapSelect<>(Int2DoubleEntrySortedByValue.class, quantileIndex);

			for (int index = 0; index < array.length; index++) {
				double value = array[index];
				heapSelect.add(new Int2DoubleEntrySortedByValue(new AbstractInt2DoubleMap.BasicEntry(index, value)));
			}

			return onDoubleArrayQuantile(heapSelect);
		} else {
			throw new IllegalArgumentException(
					"Unexpected underlying: %s".formatted(PepperLogHelper.getObjectAndClass(rawArray)));
		}
	}

	protected IValueProvider onIntArrayQuantile(HeapSelect<Int2IntEntrySortedByValue> heapSelect) {
		Int2IntMap.Entry quantileEntry = heapSelect.peek().getEntry();
		return vc -> vc.onLong(quantileEntry.getIntValue());
	}

	protected IValueProvider onDoubleArrayQuantile(HeapSelect<Int2DoubleEntrySortedByValue> heapSelect) {
		Int2DoubleMap.Entry quantileEntry = heapSelect.peek().getEntry();
		return vc -> vc.onDouble(quantileEntry.getDoubleValue());
	}

}
