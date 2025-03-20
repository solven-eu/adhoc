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
package eu.solven.adhoc.data.column;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import eu.solven.adhoc.data.cell.IValueFunction;
import eu.solven.adhoc.data.cell.IValueReceiver;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;

@Builder
public class MultitypeArray implements IMultitypeArray {
	// Indicate the single type of values stored in this column
	// For now, since column can handle long or (exclusively) doubles. Switching to Object if the type is not constant
	@Default
	byte valuesType = 0;

	// TODO How could we manage crossTypes efficiently?
	// Using an intToType could do that job, but the use of a hash-based structure makes it equivalent to
	// MergeableMultitypeStorage
	@Default
	@NonNull
	final LongList valuesL = new LongArrayList();

	@Default
	@NonNull
	final DoubleList valuesD = new DoubleArrayList();

	@Default
	@NonNull
	final List<Object> valuesO = new ArrayList<>();

	@Override
	public int size() {
		if (valuesType == IMultitypeConstants.MASK_EMPTY) {
			return 0;
		} else if (valuesType == IMultitypeConstants.MASK_LONG) {
			return valuesL.size();
		} else if (valuesType == IMultitypeConstants.MASK_DOUBLE) {
			return valuesD.size();
		} else {
			return valuesO.size();
		}
	}

	public IValueReceiver add() {
		int insertionIndex = size();

		return add(insertionIndex);
	}

	public IValueReceiver add(int insertionIndex) {
		return new IValueReceiver() {
			@Override
			public void onLong(long v) {
				if (valuesType == IMultitypeConstants.MASK_EMPTY) {
					valuesType = IMultitypeConstants.MASK_LONG;
					valuesL.add(insertionIndex, v);
				} else if (valuesType == IMultitypeConstants.MASK_LONG) {
					valuesL.add(insertionIndex, v);
				} else {
					onObject(v);
				}
			}

			@Override
			public void onDouble(double v) {
				if (valuesType == IMultitypeConstants.MASK_EMPTY) {
					valuesType = IMultitypeConstants.MASK_DOUBLE;
					valuesD.add(insertionIndex, v);
				} else if (valuesType == IMultitypeConstants.MASK_DOUBLE) {
					valuesD.add(insertionIndex, v);
				} else {
					onObject(v);
				}
			}

			@Override
			public void onObject(Object v) {
				if (v == null) {
					// BEWARE We may want to remove the key
					// BEWARE We may want to have an optimized storage for `null||long` or `null||double`
				}

				ensureObject();
				valuesO.add(insertionIndex, v);
			}
		};
	}

	protected void ensureObject() {
		if (valuesType == IMultitypeConstants.MASK_OBJECT) {
			// Already on proper type
		} else {
			if (valuesType == IMultitypeConstants.MASK_EMPTY) {
				// First value
			} else if (valuesType == IMultitypeConstants.MASK_LONG) {
				// Migrate the column from long to object
				valuesO.addAll(valuesL);
				valuesL.clear();
				// }
			} else if (valuesType == IMultitypeConstants.MASK_DOUBLE) {
				// Migrate the column from double to object
				valuesO.addAll(valuesD);
				valuesD.clear();
			} else {
				throw new IllegalStateException("valueType=%s".formatted(valuesType));
			}
			valuesType = IMultitypeConstants.MASK_OBJECT;
		}
	}

	public IValueReceiver set(int index) {
		return new IValueReceiver() {
			@Override
			public void onLong(long v) {
				if (valuesType == IMultitypeConstants.MASK_EMPTY) {
					valuesType = IMultitypeConstants.MASK_LONG;
					valuesL.set(index, v);
				} else if (valuesType == IMultitypeConstants.MASK_LONG) {
					valuesL.set(index, v);
				} else {
					onObject(v);
				}
			}

			@Override
			public void onObject(Object v) {
				ensureObject();
				valuesO.set(index, v);
			}
		};
	}

	public void read(int rowIndex, IValueReceiver valueConsumer) {
		if (valuesType == IMultitypeConstants.MASK_EMPTY) {
			throw new IndexOutOfBoundsException(rowIndex);
		} else if (valuesType == IMultitypeConstants.MASK_LONG) {
			valueConsumer.onLong(valuesL.getLong(rowIndex));
		} else if (valuesType == IMultitypeConstants.MASK_DOUBLE) {
			valueConsumer.onDouble(valuesD.getDouble(rowIndex));
		} else {
			valueConsumer.onObject(valuesO.get(rowIndex));
		}
	}

	public <U> U apply(int rowIndex, IValueFunction<U> valueFunction) {
		if (valuesType == IMultitypeConstants.MASK_EMPTY) {
			throw new IndexOutOfBoundsException(rowIndex);
		} else if (valuesType == IMultitypeConstants.MASK_LONG) {
			return valueFunction.onLong(valuesL.getLong(rowIndex));
		} else if (valuesType == IMultitypeConstants.MASK_DOUBLE) {
			return valueFunction.onDouble(valuesD.getDouble(rowIndex));
		} else {
			return valueFunction.onObject(valuesO.get(rowIndex));
		}
	}

	public static MultitypeArray empty() {
		return MultitypeArray.builder().valuesD(DoubleList.of()).valuesL(LongList.of()).valuesO(List.of()).build();
	}

	public void replaceAllObjects(Function<Object, Object> function) {
		if (valuesType == IMultitypeConstants.MASK_EMPTY) {
			// nothing
		} else if (valuesType == IMultitypeConstants.MASK_LONG) {
			// nothing
		} else if (valuesType == IMultitypeConstants.MASK_DOUBLE) {
			// nothing
		} else {
			valuesO.replaceAll(function::apply);
		}
	}

}
