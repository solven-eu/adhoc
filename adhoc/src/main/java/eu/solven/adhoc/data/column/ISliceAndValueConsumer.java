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

import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.row.slice.SliceAsMap;

/**
 * For data-structure in which we an associate a slice to a value.
 * 
 * @author Benoit Lacelle
 */
public interface ISliceAndValueConsumer {
	@Deprecated(since = "Should rely on `IValueConsumer putSlice(AdhocSliceAsMap slice)`")
	default void putSlice(SliceAsMap slice, Object value) {
		putSlice(slice).onObject(value);
	}

	/**
	 * 
	 * @param slice
	 * @return a {@link IValueReceiver} into which the value to write has to be pushed.
	 */
	IValueReceiver putSlice(SliceAsMap slice);
}
