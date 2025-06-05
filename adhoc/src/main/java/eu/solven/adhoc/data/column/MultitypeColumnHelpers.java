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

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.column.MultitypeArray.MultitypeArrayBuilder;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleImmutableList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongImmutableList;
import it.unimi.dsi.fastutil.objects.AbstractObject2DoubleMap;
import it.unimi.dsi.fastutil.objects.AbstractObject2LongMap;
import it.unimi.dsi.fastutil.objects.AbstractObject2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.experimental.UtilityClass;

/**
 * Utilities related with {@link IMultitypeColumnFastGet}.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class MultitypeColumnHelpers {

	/**
	 * 
	 * @param <T>
	 * @param input
	 * @return a copy into a {@link MultitypeNavigableColumn}
	 */
	@SuppressWarnings("PMD.LooseCoupling")
	public static <
			T extends Comparable<T>> IMultitypeColumnFastGet<T> copyToNavigable(IMultitypeColumnFastGet<T> input) {
		int size = Ints.checkedCast(input.size());

		if (size == 0) {
			return MultitypeNavigableColumn.empty();
		}

		AtomicLong nbLong = new AtomicLong();
		AtomicLong nbDouble = new AtomicLong();
		ObjectList<Map.Entry<T, ?>> keyToObject = new ObjectArrayList<>(size);

		input.stream().forEach(sm -> {
			sm.getValueProvider().acceptReceiver(new IValueReceiver() {

				@Override
				public void onLong(long v) {
					nbLong.incrementAndGet();
					keyToObject.add(new AbstractObject2LongMap.BasicEntry<>(sm.getSlice(), v));
				}

				@Override
				public void onDouble(double v) {
					nbDouble.incrementAndGet();
					keyToObject.add(new AbstractObject2DoubleMap.BasicEntry<>(sm.getSlice(), v));
				}

				@Override
				public void onObject(Object v) {
					keyToObject.add(new AbstractObject2ObjectMap.BasicEntry<>(sm.getSlice(), v));
				}
			});
		});

		MultitypeArrayBuilder multitypeArrayBuilder = MultitypeArray.builder();

		final ImmutableList.Builder<T> keys = ImmutableList.builderWithExpectedSize(size);

		// https://stackoverflow.com/questions/17328077/difference-between-arrays-sort-and-arrays-parallelsort
		// This section typically takes from 100ms to 1s for 100k slices
		keyToObject.sort((Comparator) Map.Entry.<T, Object>comparingByKey());

		if (nbLong.get() == size) {
			final LongArrayList values = new LongArrayList(size);

			keyToObject.forEach(e -> {
				keys.add(e.getKey());
				values.add(((Object2LongMap.Entry<?>) e).getLongValue());
			});

			multitypeArrayBuilder.valuesL(LongImmutableList.of(values.elements()))
					.valuesType(IMultitypeConstants.MASK_LONG);
		} else if (nbDouble.get() == size) {
			final DoubleArrayList values = new DoubleArrayList(size);

			keyToObject.forEach(e -> {
				keys.add(e.getKey());
				values.add(((Object2DoubleMap.Entry<?>) e).getDoubleValue());
			});

			multitypeArrayBuilder.valuesD(DoubleImmutableList.of(values.elements()))
					.valuesType(IMultitypeConstants.MASK_DOUBLE);
		} else {
			final ImmutableList.Builder<Object> values = ImmutableList.builderWithExpectedSize(size);

			keyToObject.forEach(e -> {
				keys.add(e.getKey());
				values.add(e.getValue());
			});

			multitypeArrayBuilder.valuesO(values.build()).valuesType(IMultitypeConstants.MASK_OBJECT);
		}

		return MultitypeNavigableColumn.<T>builder()
				.keys(keys.build())
				.values(multitypeArrayBuilder.build())
				.locked(true)
				.build();
	}
}
