/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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

import eu.solven.adhoc.primitive.IValueReceiver;

/**
 * {@link IMultitypeMergeableColumn} specialized for primitive {@code int} keys: combines the {@code merge} contract of
 * {@link IMultitypeMergeableColumn} with the unboxed {@code append}/{@code onValue} fast-paths of
 * {@link IMultitypeIntColumnFastGet}. The default {@code merge(Integer)} bridge unboxes to {@link #merge(int)} so
 * existing generic-typed call sites remain source-compatible.
 *
 * @author Benoit Lacelle
 */
public interface IMultitypeMergeableIntColumn extends IMultitypeIntColumnFastGet, IMultitypeMergeableColumn<Integer> {

	IValueReceiver merge(int key);

	@Override
	default IValueReceiver merge(Integer key) {
		return merge(key.intValue());
	}
}
