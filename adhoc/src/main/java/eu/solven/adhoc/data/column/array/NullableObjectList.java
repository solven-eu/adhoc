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

import eu.solven.adhoc.data.tabular.primitives.Int2ObjectBiConsumer;
import it.unimi.dsi.fastutil.objects.AbstractObjectList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.objects.ObjectLists;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;

/**
 * Standard {@link INullableObjectDictionary}.
 * 
 * @param <T>
 * @author Benoit Lacelle
 */
@Deprecated(since = "Not-Ready")
@Builder
public class NullableObjectList<T> extends AbstractObjectList<T> implements INullableObjectDictionary<T> {

	// Use to register the bits to skip, as not all indexes may be written in LongList
	@Default
	@NonNull
	final RoaringBitmap nullBitmap = new RoaringBitmap();

	@Default
	@NonNull
	final ObjectList<T> list = new ObjectArrayList<>();

	@Override
	public boolean isNull(int index) {
		return nullBitmap.contains(index);
	}

	@Override
	public T get(int index) {
		if (index >= size() || nullBitmap.contains(index)) {
			return null;
		}
		return list.get(index);
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
	public T set(int index, T k) {
		// e.g. if `key==1` but `size==0`, we have to skip `key==0`

		int size = list.size();
		if (size <= index) {
			// size is too small
			if (size < index) {
				list.addElements(size, (T[]) new Object[index - size]);
				nullBitmap.add(size, index - size);
			}
			list.add(k);
			return null;
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
	public T remove(int i) {
		if (i >= size()) {
			return null;
		}
		if (nullBitmap.contains(i)) {
			return null;
		}
		nullBitmap.add(i);
		return list.get(i);
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
	public void forEach(Int2ObjectBiConsumer indexToValue) {
		indexStream().forEach(index -> indexToValue.acceptInt2Object(index, list.get(index)));
	}

	public static <T> INullableObjectDictionary<T> empty() {
		return NullableObjectList.<T>builder().list(ObjectLists.emptyList()).build();
	}

}
