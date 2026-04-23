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

import java.util.Optional;

import eu.solven.adhoc.cuboid.slice.Slice;
import eu.solven.adhoc.primitive.IValueReceiver;

/**
 * For {@link IMultitypeColumn} which enables fast `.get` operations.
 * 
 * @param <T>
 *            typically {@link Slice}
 * @author Benoit Lacelle
 */
public interface IMultitypeColumnFastGetSorted<T> extends IMultitypeColumnFastGet<T> {

	/**
	 * 
	 * @param key
	 * @param distinct
	 *            if true, we are guaranteed given key is new
	 * @return an {@link IValueReceiver} if given key is higher than current max (hence new), or already present
	 *         anywhere (which is a slow path).
	 */
	Optional<IValueReceiver> appendIfOptimal(T key, boolean distinct);

	@Override
	IMultitypeColumnFastGetSorted<T> purgeAggregationCarriers();
}
