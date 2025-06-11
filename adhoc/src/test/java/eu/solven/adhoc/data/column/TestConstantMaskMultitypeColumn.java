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

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.data.row.slice.SliceAsMap;

public class TestConstantMaskMultitypeColumn {
	@Test
	public void testGetValue() {
		Map<String, ?> mask = Map.of("k2", "v2");

		MultitypeHashColumn<SliceAsMap> withoutMask = MultitypeHashColumn.<SliceAsMap>builder().build();

		withoutMask.set(SliceAsMap.fromMap(Map.of())).onLong(123);

		ConstantMaskMultitypeColumn withMask =
				ConstantMaskMultitypeColumn.builder().masked(withoutMask).masks(mask).build();

		Assertions.assertThat(IValueProvider.getValue(withMask.onValue(SliceAsMap.fromMap(Map.of())))).isEqualTo(null);
		Assertions.assertThat(IValueProvider.getValue(withMask.onValue(SliceAsMap.fromMap(Map.of("k2", "v2")))))
				.isEqualTo(123L);
	}

	@Test
	public void testConflictingColumns() {
		Map<String, ?> mask = Map.of("k", "v2");

		MultitypeHashColumn<SliceAsMap> withoutMask = MultitypeHashColumn.<SliceAsMap>builder().build();

		withoutMask.set(SliceAsMap.fromMap(Map.of("k", "v1"))).onLong(123);

		ConstantMaskMultitypeColumn withMask =
				ConstantMaskMultitypeColumn.builder().masked(withoutMask).masks(mask).build();

		Assertions.assertThat(IValueProvider.getValue(withMask.onValue(SliceAsMap.fromMap(Map.of())))).isEqualTo(null);
		Assertions.assertThat(IValueProvider.getValue(withMask.onValue(SliceAsMap.fromMap(Map.of("k", "v1")))))
				.isEqualTo(null);
		Assertions.assertThat(IValueProvider.getValue(withMask.onValue(SliceAsMap.fromMap(Map.of("k", "v2")))))
				.isEqualTo(null);

		Assertions.assertThatThrownBy(() -> withMask.stream().toList()).isInstanceOf(IllegalArgumentException.class);
	}
}
