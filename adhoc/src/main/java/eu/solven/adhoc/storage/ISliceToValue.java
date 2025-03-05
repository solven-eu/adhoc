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
package eu.solven.adhoc.storage;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import eu.solven.adhoc.measure.transformator.ITransformator;
import eu.solven.adhoc.slice.IAdhocSlice;
import eu.solven.adhoc.slice.SliceAsMap;

/**
 * A {@link ISliceToValue} is an immutable data-structure, expressing the mapping from slices to values, typically
 * computed by a {@link ITransformator}.
 */
public interface ISliceToValue {
	Stream<SliceAsMap> keySetStream();

	Set<SliceAsMap> slicesSet();

	long size();

	void onValue(SliceAsMap slice, IValueConsumer consumer);

	void forEachSlice(IRowScanner<SliceAsMap> rowScanner);

	<U> Stream<U> stream(IRowConverter<SliceAsMap, U> rowScanner);

	/**
	 * 
	 * @param <T>
	 * @param storage
	 * @param slice
	 * @return the value as {@link Object} on given slice
	 */
	static <T> Object getValue(ISliceToValue storage, IAdhocSlice slice) {
		AtomicReference<Object> refV = new AtomicReference<>();

		storage.onValue(slice.getAdhocSliceAsMap(), refV::set);

		return refV.get();
	}

}
