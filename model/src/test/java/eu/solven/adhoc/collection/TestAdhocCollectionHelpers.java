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
package eu.solven.adhoc.collection;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAdhocCollectionHelpers {

	/**
	 * A {@link Collection} that also implements {@link ICompactable} but is NOT an {@link ArrayList}, so
	 * {@link AdhocCollectionHelpers#trimToSize} reaches the {@link ICompactable} branch.
	 */
	private static class CompactableCollection<E> extends AbstractCollection<E> implements ICompactable {
		private final List<E> delegate = new ArrayList<>();
		boolean compacted;

		@Override
		public Iterator<E> iterator() {
			return delegate.iterator();
		}

		@Override
		public int size() {
			return delegate.size();
		}

		@Override
		public boolean add(E e) {
			return delegate.add(e);
		}

		@Override
		public void compact() {
			compacted = true;
		}
	}

	// --- trimToSize ---

	@Test
	public void testTrimToSize_arrayList_doesNotThrow() {
		ArrayList<String> list = new ArrayList<>();
		list.add("a");
		list.add("b");

		// trimToSize on ArrayList must not throw and must leave elements intact
		AdhocCollectionHelpers.trimToSize(list);

		Assertions.assertThat(list).containsExactly("a", "b");
	}

	@Test
	public void testTrimToSize_compactable_invokesCompact() {
		CompactableCollection<String> col = new CompactableCollection<>();
		col.add("x");

		Assertions.assertThat(col.compacted).isFalse();

		AdhocCollectionHelpers.trimToSize(col);

		Assertions.assertThat(col.compacted).isTrue();
		// elements must be untouched
		Assertions.assertThat(col).containsExactly("x");
	}

	@Test
	public void testTrimToSize_compactable_empty_invokesCompact() {
		CompactableCollection<String> col = new CompactableCollection<>();

		AdhocCollectionHelpers.trimToSize(col);

		Assertions.assertThat(col.compacted).isTrue();
	}

	@Test
	public void testTrimToSize_unknownCollection_isNoOp() {
		// LinkedList is neither ArrayList nor ICompactable — trimToSize must be a no-op
		Collection<String> list = new LinkedList<>(List.of("a", "b"));

		AdhocCollectionHelpers.trimToSize(list);

		Assertions.assertThat(list).containsExactly("a", "b");
	}
}
