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
package eu.solven.adhoc.data.column.array;

import java.util.stream.IntStream;

import org.roaringbitmap.RoaringBitmap;

import eu.solven.adhoc.data.column.ICompactable;
import eu.solven.adhoc.data.tabular.primitives.Int2LongBiConsumer;
import eu.solven.adhoc.util.NotYetImplementedException;
import it.unimi.dsi.fastutil.longs.AbstractLongList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import it.unimi.dsi.fastutil.longs.LongSpliterator;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;

/**
 * Adds `null` capabilities to a {@link LongList}.
 * 
 * @author Benoit Lacelle
 */
@Deprecated(since = "Not-Ready")
@Builder
public class NullableLongList extends AbstractLongList implements INullableLongList, ICompactable {

	// Use to register the bits to skip, as not all indexes may be written in LongList
	@Default
	@NonNull
	final RoaringBitmap nullBitmap = new RoaringBitmap();

	@Default
	@NonNull
	final LongList list = new LongArrayList();

	@Override
	public boolean isNull(int index) {
		return nullBitmap.contains(index);
	}

	@Override
	public long getLong(int index) {
		if (index < 0 || index >= size() || nullBitmap.contains(index)) {
			return Long.MIN_VALUE;
		}
		return list.getLong(index);
	}

	@Override
	public int size() {
		return list.size();
	}

	@Override
	public int sizeNotNull() {
		return size() - nullBitmap.getCardinality();
	}

	@Override
	public boolean add(long k) {
		set(size(), k);

		return true;
	}

	@Override
	public boolean addNull() {
		nullBitmap.add(size());
		list.add(0L);

		return true;
	}

	@Override
	public long set(int index, long k) {
		// e.g. if `key==1` but `size==0`, we have to skip `key==0`

		int size = list.size();
		if (size <= index) {
			// size is too small
			if (size < index) {
				list.addElements(size, new long[index - size]);
				nullBitmap.add(size, index - size);
			}
			list.add(k);
			return 0;
		} else {
			// size is big enough
			nullBitmap.remove(index);
			return list.set(index, k);
		}
	}

	/**
	 * Removing is setting to null.
	 */
	@Override
	public long removeLong(int i) {
		if (i >= size()) {
			return 0L;
		}
		if (nullBitmap.contains(i)) {
			return 0L;
		}
		nullBitmap.add(i);
		return list.getLong(i);
	}

	@Override
	public boolean containsIndex(int index) {
		return index < size() && !isNull(index);
	}

	@Override
	public IntStream indexStream() {
		return IntStream.range(0, size()).filter(i -> !isNull(i));
	}

	@Override
	public void forEach(Int2LongBiConsumer indexToValue) {
		indexStream().forEach(index -> indexToValue.acceptInt2Long(index, list.getLong(index)));
	}

	@Override
	public LongSpliterator spliterator() {
		throw new NotYetImplementedException("TODO");
	}

	public static INullableLongList empty() {
		return NullableLongList.builder().list(LongLists.emptyList()).build();
	}

	@Override
	@SuppressWarnings("PMD.LooseCoupling")
	public void compact() {
		if (list instanceof LongArrayList arrayList) {
			arrayList.trim();
		}
		nullBitmap.runOptimize();
	}

	@Override
	public INullableLongList duplicate() {
		return NullableLongList.builder()
				.list(new LongArrayList(list.toLongArray()))
				.nullBitmap(nullBitmap.clone())
				.build();
	}

}
