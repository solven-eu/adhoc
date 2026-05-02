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
package eu.solven.adhoc.dataframe.column.navigable_else_hash;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.cuboid.StreamStrategy;
import eu.solven.adhoc.primitive.IValueProvider;

public class TestMultitypeNavigableElseHashColumn {
	@Test
	public void testPutUnordered() {
		MultitypeNavigableElseHashColumn<String> column = MultitypeNavigableElseHashColumn.<String>builder().build();

		// Ordered
		column.append("a").onLong(123);
		column.append("c").onLong(345);

		// Unordered
		column.append("b").onLong(234);
		// May be ordered
		column.append("e").onLong(567);
		// Unordered
		column.append("d").onLong(456);

		// the hash section has no guaranteed order
		Assertions.assertThat(column.keyStream().toList()).startsWith("a", "c", "e").contains("b", "d");

		Assertions.assertThat(column.navigable.keyStream().toList()).containsExactly("a", "c", "e");
		Assertions.assertThat(column.hash.keyStream().toList()).contains("b", "d").hasSize(2);
	}

	// ---- onValue(T, StreamStrategy) — replaces the deprecated ICanReadSortedSubComplement contract ----
	// The navigable side IS the sorted leg; the hash side IS the unordered complement.

	@Test
	public void testOnValueStrategy_routesToNavigableOrHash() {
		MultitypeNavigableElseHashColumn<String> column = MultitypeNavigableElseHashColumn.<String>builder().build();

		// Sorted insertion → goes to navigable side.
		column.append("a").onLong(1L);
		column.append("c").onLong(3L);

		// Unordered insertion → goes to hash side.
		column.append("b").onLong(2L);

		// SORTED_SUB hits the navigable side only.
		Assertions.assertThat(IValueProvider.getValue(column.onValue("a", StreamStrategy.SORTED_SUB))).isEqualTo(1L);
		Assertions.assertThat(IValueProvider.getValue(column.onValue("c", StreamStrategy.SORTED_SUB))).isEqualTo(3L);
		Assertions.assertThat(IValueProvider.getValue(column.onValue("b", StreamStrategy.SORTED_SUB))).isNull();

		// SORTED_SUB_COMPLEMENT hits the hash side only.
		Assertions.assertThat(IValueProvider.getValue(column.onValue("a", StreamStrategy.SORTED_SUB_COMPLEMENT)))
				.isNull();
		Assertions.assertThat(IValueProvider.getValue(column.onValue("c", StreamStrategy.SORTED_SUB_COMPLEMENT)))
				.isNull();
		Assertions.assertThat(IValueProvider.getValue(column.onValue("b", StreamStrategy.SORTED_SUB_COMPLEMENT)))
				.isEqualTo(2L);

		// ALL hits whichever side has the slice.
		Assertions.assertThat(IValueProvider.getValue(column.onValue("a", StreamStrategy.ALL))).isEqualTo(1L);
		Assertions.assertThat(IValueProvider.getValue(column.onValue("b", StreamStrategy.ALL))).isEqualTo(2L);
	}
}
