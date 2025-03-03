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
package eu.solven.adhoc.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.measure.sum.IAggregationCarrier;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * This data-structures aggregates input value on a per-key basis. Different keys are allowed to be associated to
 * different types (e.g. we may have some keys holding a functional double, while other keys may hold an error String).
 * <p>
 * This data-structure does not maintain order. Typically `SUM(123, 'a', 234)` could lead to `'123a234'` or `'357a'`.
 *
 * @param <T>
 */
@Builder
@Slf4j
@Deprecated(since = "Coding not finished. Not yet ready")
public class AppendableMultiTypeStorage<T> implements IMultitypeColumn<T> {

	// TODO If we were ordering keys, we could have relatively fast .onValue
	@Default
	@NonNull
	final List<T> keys = new ArrayList<>();

	// TODO How could we manage different types efficiently?
	// Using an intToType could do that job, but the use of a hash-based structure makes it as slow as
	// MergeableMultytypeStorage
	@Default
	@NonNull
	final List<Object> valuesO = new ArrayList<>();

	/**
	 * A put operation: it resets the values for given key, initializing it to the provided value.
	 *
	 * @param key
	 * @param v
	 *            if null, this behave like `.clear`
	 */
	@Override
	public IValueConsumer append(T key) {
		return v -> {
			keys.add(key);
			valuesO.add(v);

			// Do not check the assertion on each .put else it would get wuite slow
			if (Integer.bitCount(keys.size()) == 1) {
				assert keys.stream().distinct().count() == keys.size() : "multiple .put with same key is illegal";
			}
		};
	}

	@Override
	public void scan(IRowScanner<T> rowScanner) {
		int size = keys.size();
		for (int i = 0; i < size; i++) {
			rowScanner.onKey(keys.get(i)).onObject(valuesO.get(i));
		}
	}

	@Override
	public <U> Stream<U> stream(IRowConverter<T, U> converter) {
		return LongStream.range(0, size())
				.mapToInt(Ints::checkedCast)
				.mapToObj(i -> converter.convertObject(keys.get(0), valuesO.get(0)));
	}

	@Override
	public long size() {
		return keys.size();
	}

	@Override
	public Stream<T> keySetStream() {
		// No need for .distinct as each key is guaranteed to appear in a single column
		return keys.stream();

	}

	@Override
	public String toString() {
		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);

		AtomicInteger index = new AtomicInteger();

		stream((slice, v) -> Map.of("k", slice, "v", v)).limit(AdhocUnsafe.limitOrdinalToString)
				.forEach(sliceToValue -> {
					Object o = sliceToValue.get("v");
					toStringHelper.add("#" + index.getAndIncrement(), PepperLogHelper.getObjectAndClass(o));
				});

		return toStringHelper.toString();
	}

	/**
	 * @return an empty and immutable MultiTypeStorage
	 */
	public static <T> AppendableMultiTypeStorage<T> empty() {
		return AppendableMultiTypeStorage.<T>builder()
				.keys(Collections.emptyList())
				.valuesO(Collections.emptyList())
				.build();
	}

	@Override
	public void purgeAggregationCarriers() {
		LongStream.range(0, size()).mapToInt(Ints::checkedCast).forEach(i -> {
			Object value = valuesO.get(i);

			if (value instanceof IAggregationCarrier aggregationCarrier) {
				aggregationCarrier.acceptValueConsumer(new IValueConsumer() {

					@Override
					public void onObject(Object object) {
						// Replace current value
						valuesO.set(i, object);
					}
				});
			}
		});
	}
}
