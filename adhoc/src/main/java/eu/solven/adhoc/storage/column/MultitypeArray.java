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
package eu.solven.adhoc.storage.column;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import eu.solven.adhoc.storage.IValueConsumer;
import eu.solven.adhoc.storage.IValueFunction;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;

@Builder
public class MultitypeArray {
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

	public int size() {
		if (valuesType == 0) {
			return 0;
		} else if (valuesType == 1) {
			return valuesL.size();
		} else if (valuesType == 2) {
			return valuesD.size();
		} else {
			return valuesO.size();
		}
	}

	public IValueConsumer add() {
		int insertionIndex = size();

		return add(insertionIndex);
	}

	public IValueConsumer add(int insertionIndex) {
		return new IValueConsumer() {
			@Override
			public void onLong(long v) {
				if (valuesType == 0) {
					valuesType = 1;
					valuesL.add(v);
				} else if (valuesType == 1) {
					valuesL.add(v);
				} else {
					onObject(v);
				}
			}

			@Override
			public void onObject(Object v) {
				if (valuesType == 0) {
					// First value
					valuesType = 8;
				} else if (valuesType == 8) {
				} else if (valuesType == 1) {
					// if (SumAggregation.isLongLike(v)) {
					// onLong(SumAggregation.asLong(v));
					// } else {
					// Migrate the column from long to object
					valuesL.iterator().forEachRemaining(longV -> valuesO.add(longV));
					valuesL.clear();
					// }
				} else if (valuesType == 2) {
					// Migrate the column from double to object
					valuesD.iterator().forEachRemaining(doubleV -> valuesO.add(doubleV));
					valuesD.clear();
				} else {
					throw new IllegalStateException("valueType=%s".formatted(valuesType));
				}
				valuesO.add(v);
			}
		};
	}

	public IValueConsumer set(int index) {
		return new IValueConsumer() {
			@Override
			public void onLong(long v) {
				if (valuesType == 0) {
					valuesType = 1;
					valuesL.set(index, v);
				} else if (valuesType == 1) {
					valuesL.set(index, v);
				} else {
					onObject(v);
				}
			}

			@Override
			public void onObject(Object v) {
				if (valuesType == 0) {
					// First value
					valuesType = 8;
				} else if (valuesType == 8) {
				} else if (valuesType == 1) {
					// Migrate the column from long to object
					valuesL.iterator().forEachRemaining(longV -> valuesO.add(longV));
					valuesL.clear();
					valuesType = 8;
					// }
				} else if (valuesType == 2) {
					// Migrate the column from double to object
					valuesD.iterator().forEachRemaining(doubleV -> valuesO.add(doubleV));
					valuesD.clear();
					valuesType = 8;
				} else {
					throw new IllegalStateException("valueType=%s".formatted(valuesType));
				}
				valuesO.set(index, v);
			}
		};
	}

	public void read(int rowIndex, IValueConsumer valueConsumer) {
		if (valuesType == 0) {
			throw new IndexOutOfBoundsException(rowIndex);
		} else if (valuesType == 1) {
			valueConsumer.onLong(valuesL.getLong(rowIndex));
		} else if (valuesType == 2) {
			valueConsumer.onDouble(valuesD.getDouble(rowIndex));
		} else {
			valueConsumer.onObject(valuesO.get(rowIndex));
		}
	}

	public <U> U apply(int rowIndex, IValueFunction<U> valueFunction) {
		if (valuesType == 0) {
			throw new IndexOutOfBoundsException(rowIndex);
		} else if (valuesType == 1) {
			return valueFunction.onLong(valuesL.getLong(rowIndex));
		} else if (valuesType == 2) {
			return valueFunction.onDouble(valuesD.getDouble(rowIndex));
		} else {
			return valueFunction.onObject(valuesO.get(rowIndex));
		}
	}

	public static MultitypeArray empty() {
		return MultitypeArray.builder().valuesD(DoubleList.of()).valuesL(LongList.of()).valuesO(List.of()).build();
	}

	public void replaceObjects(Function<Object, Object> function) {
		if (valuesType == 0) {
			// nothing
		} else if (valuesType == 1) {
			// nothing
		} else if (valuesType == 2) {
			// nothing
		} else {
			valuesO.replaceAll(v -> {
				return function.apply(v);
			});
		}
	}

}
