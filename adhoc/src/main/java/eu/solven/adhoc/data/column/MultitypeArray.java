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
import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier;
import eu.solven.adhoc.util.AdhocUnsafe;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
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
public class MultitypeArray implements IMultitypeArray {
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
	final LongList valuesL = new LongArrayList(0);

	@Default
	@NonNull
	final DoubleList valuesD = new DoubleArrayList(0);

	@Default
	@NonNull
	final List<Object> valuesO = new ArrayList<>(0);

	@Default
	final int capacity = IAdhocCapacityConstants.ZERO_THEN_MAX;

	/**
	 * To be called before a guaranteed `add` operation.
	 */
	protected void checkSizeBeforeAdd(int type) {
		long size = size();
		if (size >= AdhocUnsafe.limitColumnSize) {
			// TODO Log the first and last elements
			throw new IllegalStateException(
					"Can not add as size=%s and limit=%s".formatted(size, AdhocUnsafe.limitColumnSize));
		} else if (size == 0) {
			ensureCapacityForType(type);
		}
	}

	@SuppressWarnings({ "PMD.LooseCoupling", "PMD.CollapsibleIfStatements" })
	protected void ensureCapacityForType(int type) {
		if (type == IMultitypeConstants.MASK_LONG) {
			if (valuesL instanceof LongArrayList array) {
				array.ensureCapacity(IAdhocCapacityConstants.capacity(capacity));
			}
		} else if (type == IMultitypeConstants.MASK_DOUBLE) {
			if (valuesD instanceof DoubleArrayList array) {
				array.ensureCapacity(IAdhocCapacityConstants.capacity(capacity));
			}
		} else if (type == IMultitypeConstants.MASK_OBJECT) {
			if (valuesO instanceof ObjectArrayList array) {
				array.ensureCapacity(IAdhocCapacityConstants.capacity(capacity));
			}
		}
	}

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

	@Override
	public IValueReceiver add() {
		int insertionIndex = size();

		return add(insertionIndex);
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
		return MultitypeArray.builder().valuesD(DoubleList.of()).valuesL(LongList.of()).valuesO(List.of()).build();
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

	public void clear() {
		valuesType = IMultitypeConstants.MASK_EMPTY;

		valuesL.clear();
		valuesD.clear();
		valuesO.clear();
	}

	@Override
	public IMultitypeArray purgeAggregationCarriers() {
		final List<Object> valuesOPurged = new ArrayList<>(valuesO);

		for (int i = 0; i < valuesO.size(); i++) {
			Object value = valuesOPurged.get(i);
			if (value instanceof IAggregationCarrier aggregationCarrier) {
				valuesO.set(i, IValueProvider.getValue(aggregationCarrier::acceptValueReceiver));
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
