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
package eu.solven.adhoc.dataframe.column.navigable;

import eu.solven.adhoc.dataframe.column.IMultitypeMergeableIntColumn;
import eu.solven.adhoc.dataframe.column.merge.MergingNavigableValueReceiver;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.primitive.IValueReceiver;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Int-specialized companion to {@link MultitypeNavigableMergeableColumn}: stores ({@code int key} -> aggregated
 * multitype value) pairs in a primitive {@link it.unimi.dsi.fastutil.ints.IntArrayList}, eliminating the
 * {@link Integer} boxing that the generic mergeable variant otherwise incurs on every {@code append} / {@code merge}
 * call. Designed as the storage column of {@code AggregatingColumns} when the dictionarization indices are guaranteed
 * to fit into an {@code int}.
 *
 * @author Benoit Lacelle
 */
// CPD-OFF — the inner-receiver chains intentionally mirror MultitypeNavigableMergeableColumn.
@SuppressWarnings("CPD-START")
@SuperBuilder
@Slf4j
public class MultitypeNavigableMergeableIntColumn extends MultitypeNavigableIntColumn
		implements IMultitypeMergeableIntColumn {

	@NonNull
	@Getter
	IAggregation aggregation;

	/**
	 * @param index
	 *            the index of the existing entry whose value is being merged into.
	 * @param key
	 *            the duplicated key (used only for the parent's not-locked check via the surrounding callers).
	 * @return an {@link IValueReceiver} that accepts the incoming value and writes the aggregated result back to
	 *         {@code values[index]}.
	 */
	@Override
	protected IValueReceiver merge(int index, int key) {
		checkNotLocked(key);

		IValueReceiver receiver = values.set(index);

		return new MergingNavigableValueReceiver(aggregation, receiver, r -> onValue(index, r));
	}

	@Override
	public IValueReceiver merge(int key) {
		// TODO If the key is absent, current implementation should look for insertionIndex right away
		int index = getIndex(key);

		if (index < 0) {
			return append(key);
		}
		return merge(index, key);
	}
}
