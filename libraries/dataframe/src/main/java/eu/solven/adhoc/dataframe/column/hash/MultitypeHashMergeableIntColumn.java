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
package eu.solven.adhoc.dataframe.column.hash;

import eu.solven.adhoc.dataframe.column.IMultitypeMergeableIntColumn;
import eu.solven.adhoc.dataframe.column.merge.IIntColumnPutter;
import eu.solven.adhoc.dataframe.column.merge.MergingIntColumnValueReceiver;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.primitive.IValueReceiver;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * This data-structures aggregates input value on a per-key basis. Different keys are allowed to be associated to
 * different types (e.g. we may have some keys holding a functional double, while other keys may hold an error String).
 * <p>
 * This data-structure does not maintain order. Typically `SUM(123, 'a', 234)` could lead to `'123a234'` or `'357a'`.
 *
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
public class MultitypeHashMergeableIntColumn extends MultitypeHashIntColumn
		implements IMultitypeMergeableIntColumn, IIntColumnPutter {

	@NonNull
	@Getter
	IAggregation aggregation;

	@Override
	public IValueReceiver unsafePut(int key, boolean safe) {
		// from protected to public
		return super.unsafePut(key, safe);
	}

	@Override
	public void removeLong(int key) {
		sliceToL.remove(key);
	}

	@Override
	public void removeDouble(int key) {
		sliceToD.remove(key);
	}

	@Override
	public IValueReceiver merge(int key) {
		return new MergingIntColumnValueReceiver(aggregation, this, key);
	}
}
