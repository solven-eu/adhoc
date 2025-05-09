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

import java.util.function.Function;

import eu.solven.adhoc.data.cell.IValueFunction;
import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;

/**
 * A Data-structure similar to an array, enable multitype.
 * 
 * @author Benoit Lacelle
 */
public interface IMultitypeArray {

	int size();

	/**
	 * Append a value at the end of this array.
	 * 
	 * @return
	 */
	default IValueReceiver add() {
		return add(size());
	}

	/**
	 * 
	 * @param insertionIndex
	 *            must be between `0` and `size()` included.
	 * @return a {@link IValueReceiver} to push the value to write
	 */
	IValueReceiver add(int insertionIndex);

	/**
	 * 
	 * @param rowIndex
	 *            the rowIndex at which the value has to be changed. Must be between `0` and `size()` excluded.
	 * @return
	 */
	IValueReceiver set(int rowIndex);

	IValueProvider read(int rowIndex);

	<U> U apply(int rowIndex, IValueFunction<U> valueFunction);

	// BEWARE Should rely on IValueConsumer
	// Relates with IAggregationCarrier
	void replaceAllObjects(Function<Object, Object> function);

	@Deprecated(since = "Generally slow. Use for uniTests or specific cases")
	void remove(int index);

}
