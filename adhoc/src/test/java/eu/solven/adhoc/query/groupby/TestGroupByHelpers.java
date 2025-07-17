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
package eu.solven.adhoc.query.groupby;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.IValueProviderTestHelpers;
import eu.solven.adhoc.data.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.primitive.IValueProvider;

public class TestGroupByHelpers {
	@Test
	public void testMask() {
		IMultitypeColumnFastGet<IAdhocSlice> column = MultitypeHashColumn.<IAdhocSlice>builder().build();
		IMultitypeColumnFastGet<IAdhocSlice> withMask = GroupByHelpers.addConstantColumns(column, Map.of("k2", "v2"));

		column.append(SliceAsMap.fromMap(Map.of("k", "v"))).onLong(123L);

		Assertions
				.assertThat(IValueProviderTestHelpers
						.getLong(withMask.onValue(SliceAsMap.fromMap(Map.of("k", "v", "k2", "v2")))))
				.isEqualTo(123L);

		Assertions
				.assertThat(IValueProvider
						.getValue(withMask.onValue(SliceAsMap.fromMap(Map.of("k", "v", "k2", "unknownValue")))))
				.isNull();
	}
}
