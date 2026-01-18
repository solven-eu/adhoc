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
package eu.solven.adhoc.compression.page;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableRowHelpers {
	@Test
	public void testTableRow_emptyRow() {
		ITableRowWrite emptyRow = TableRowHelpers.emptyRow();
		Assertions.assertThat(emptyRow)
				.hasToString("empty")
				.isSameAs(TableRowHelpers.emptyRow())
				.hasSameHashCodeAs(List.of())
				.isEqualTo(new ITableRowWrite() {

					@Override
					public int size() {
						return 0;
					}

					@Override
					public ITableRowRead freeze() {
						throw new UnsupportedOperationException();
					}

					@Override
					public int add(String key, Object value) {
						throw new UnsupportedOperationException();
					}
				});
	}

	@Test
	public void testTableRow_empty() {
		ITableRowRead emptyRow = TableRowHelpers.emptyRead();
		Assertions.assertThat(emptyRow)
				.hasToString("empty")
				.isSameAs(TableRowHelpers.emptyRead())
				.hasSameHashCodeAs(List.of())
				.isEqualTo(new ITableRowRead() {

					@Override
					public int size() {
						return 0;
					}

					@Override
					public Object readValue(int columnIndex) {
						throw new UnsupportedOperationException();
					}
				});
	}
}
