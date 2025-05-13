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

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.row.slice.SliceAsMap;

/**
 * For {@link IMultitypeColumn} which enables fast `.get` operations.
 * 
 * @param <T>
 *            typically {@link SliceAsMap}
 * @author Benoit Lacelle
 */
public interface IMultitypeColumnFastGet<T> extends IMultitypeColumn<T> {

	/**
	 * Similar to a `.get` but the value is available through a {@link IValueReceiver}
	 * 
	 * @param slice
	 * @param valueReceiver
	 */
	@Deprecated(since = "Prefer `IValueProvider onValue(T key)`")
	default void onValue(T slice, IValueReceiver valueReceiver) {
		onValue(slice).acceptReceiver(valueReceiver);
	}

	IValueProvider onValue(T key);

	/**
	 * Similar to a `.set` but the value is available through a {@link IValueReceiver}
	 * 
	 * @param slice
	 * @param valueReceiver
	 */
	@Deprecated(since = "Prefer `IValueProvider onValue(T key)`")
	default void set(T slice, Object o) {
		set(slice).onObject(o);
	}

	/**
	 * 
	 * @param key
	 * @return a IValueReceiver to provide the value
	 */
	IValueReceiver set(T key);

}
