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
package eu.solven.adhoc.dataframe.column;

import java.util.function.Function;

import eu.solven.adhoc.collection.ICompactable;
import eu.solven.adhoc.dataframe.IAdhocCapacityConstants;
import eu.solven.adhoc.dataframe.collection.ChunkedList;
import eu.solven.adhoc.dataframe.collection.DoubleChunkedList;
import eu.solven.adhoc.dataframe.collection.LongChunkedList;
import eu.solven.adhoc.encoding.column.AdhocColumnUnsafe;
import eu.solven.adhoc.primitive.IMultitypeConstants;
import eu.solven.adhoc.primitive.IValueFunction;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Standard {@link IMultitypeArray}.
 * 
 * It accept writing different types, but the storage is mono-type. Hence, it typically switch on {@link Object} when
 * encountering different types.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@Builder
// concrete field types (LongChunkedList, DoubleChunkedList, ChunkedList) are intentional: compact() is only on the
// concrete class
@SuppressWarnings("PMD.LooseCoupling")
public class MultitypeArray implements IMultitypeArray, ICompactable {
	// Indicate the single type of values stored in this column
	// For now, since column can handle long or (exclusively) doubles. Switching to Object if the type is not only-long
	// or only-double.
	@Default
	byte valuesType = 0;

	// TODO How could we manage crossTypes efficiently?
	// Using an intToType could do that job, but the use of a hash-based structure makes it equivalent to
	// MergeableMultitypeStorage
	@Default
	@NonNull
	final LongChunkedList valuesL = new LongChunkedList();

	@Default
	@NonNull
	final DoubleChunkedList valuesD = new DoubleChunkedList();

	@Default
	@NonNull
	final ChunkedList<Object> valuesO = new ChunkedList<>();

	@Default
	@Setter
	int capacity = IAdhocCapacityConstants.ZERO_THEN_MAX;

	/**
	 * To be called before a guaranteed `add` operation.
	 */
	protected void checkSizeBeforeAdd(int type) {
		long size = size();

		AdhocColumnUnsafe.checkColumnSize(size);

		if (size == 0) {
			ensureCapacityForType(type);
		}
	}

	/**
	 * No-op: {@link LongChunkedList}, {@link DoubleChunkedList}, and {@link ChunkedList} allocate tail chunks lazily on
	 * demand; there is no bulk pre-allocation step equivalent to {@code ensureCapacity}.
	 */
	protected void ensureCapacityForType(int type) {
		// intentionally empty
	}

	@Override
	public int size() {
		return switch (valuesType) {
		case IMultitypeConstants.MASK_EMPTY -> 0;
		case IMultitypeConstants.MASK_LONG -> valuesL.size();
		case IMultitypeConstants.MASK_DOUBLE -> valuesD.size();
		default -> valuesO.size();
		};
	}

	@Override
	public IValueReceiver add(int insertionIndex) {
		return new IValueReceiver() {
			@Override
			public void onLong(long v) {
				if (valuesType == IMultitypeConstants.MASK_EMPTY) {
					checkSizeBeforeAdd(IMultitypeConstants.MASK_LONG);
					valuesType = IMultitypeConstants.MASK_LONG;
					// ensure
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
					checkSizeBeforeAdd(IMultitypeConstants.MASK_DOUBLE);
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
					log.trace("TODO Improve null management");
				}

				ensureObject();

				checkSizeBeforeAdd(IMultitypeConstants.MASK_OBJECT);
				valuesO.add(insertionIndex, v);
			}
		};
	}

	protected void ensureObject() {
		if (valuesType == IMultitypeConstants.MASK_OBJECT) {
			log.trace("Already on proper type");
		} else {
			checkSizeBeforeAdd(IMultitypeConstants.MASK_OBJECT);

			if (valuesType == IMultitypeConstants.MASK_EMPTY) {
				log.trace("First value");
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

	@Override
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

	@Override
	public IValueProvider read(int rowIndex) {
		if (valuesType == IMultitypeConstants.MASK_EMPTY) {
			throw new IndexOutOfBoundsException(rowIndex);
		}

		return new IValueProvider() {

			@Override
			public void acceptReceiver(IValueReceiver valueReceiver) {
				if (valuesType == IMultitypeConstants.MASK_LONG) {
					valueReceiver.onLong(valuesL.getLong(rowIndex));
				} else if (valuesType == IMultitypeConstants.MASK_DOUBLE) {
					valueReceiver.onDouble(valuesD.getDouble(rowIndex));
				} else {
					valueReceiver.onObject(valuesO.get(rowIndex));
				}
			}
		};
	}

	@Override
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
		return MultitypeArray.builder()
				.valuesL(new LongChunkedList())
				.valuesD(new DoubleChunkedList())
				.valuesO(new ChunkedList<>())
				.build();
	}

	@Override
	public void replaceAllObjects(Function<Object, Object> function) {
		if (valuesType == IMultitypeConstants.MASK_OBJECT) {
			valuesO.replaceAll(function::apply);
		}
		// else no object
	}

	@Override
	public void remove(int index) {
		if (valuesType == IMultitypeConstants.MASK_LONG) {
			valuesL.removeLong(index);
		} else if (valuesType == IMultitypeConstants.MASK_DOUBLE) {
			valuesD.removeDouble(index);
		} else if (valuesType == IMultitypeConstants.MASK_OBJECT) {
			valuesO.remove(index);
		}
		// else already empty
	}

	@Override
	public String toString() {
		if (valuesType == IMultitypeConstants.MASK_EMPTY) {
			return "empty";
		} else if (valuesType == IMultitypeConstants.MASK_LONG) {
			// TODO limit to first elements
			return "type=long values=" + valuesL;
		} else if (valuesType == IMultitypeConstants.MASK_DOUBLE) {
			// TODO limit to first elements
			return "type=double values=" + valuesD;
		} else {
			// TODO limit to first elements
			return "type=object values=" + valuesO;
		}
	}

	@Override
	public void compact() {
		valuesL.compact();
		valuesD.compact();
		valuesO.compact();
	}

	public void clear() {
		valuesType = IMultitypeConstants.MASK_EMPTY;

		valuesL.clear();
		valuesD.clear();
		valuesO.clear();
	}

	@Override
	public IMultitypeArray purgeAggregationCarriers() {
		final ChunkedList<Object> valuesOPurged = new ChunkedList<>(valuesO);

		for (int i = 0; i < valuesO.size(); i++) {
			Object value = valuesOPurged.get(i);
			if (value instanceof IValueProvider valueProvider) {
				valuesOPurged.set(i, IValueProvider.getValue(valueProvider));
			}
		}

		return MultitypeArray.builder()
				.valuesType(valuesType)
				.valuesL(valuesL)
				.valuesD(valuesD)
				.valuesO(valuesOPurged)
				.build();
	}
}
