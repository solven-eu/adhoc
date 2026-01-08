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
package eu.solven.adhoc.dictionary.page;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.util.immutable.UnsupportedAsImmutableException;
import lombok.experimental.UtilityClass;

/**
 * Helpers around {@link ITableRowRead}
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class TableRowHelpers {
	private static final ITableRowRead EMPTY_ROW_READ = new ITableRowRead() {

		@Override
		public int size() {
			return 0;
		}

		@Override
		public Object readValue(int columnIndex) {
			return null;
		}

		@Override
		public String toString() {
			return "empty";
		}

		@Override
		public int hashCode() {
			return ImmutableList.of().hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			} else if (obj instanceof ITableRowRead otherRow) {
				return otherRow.size() == 0;
			} else {
				return false;
			}
		}
	};

	private static final ITableRowWrite EMPTY_ROW_WRITE = new ITableRowWrite() {

		@Override
		public int size() {
			return 0;
		}

		@Override
		public ITableRowRead freeze() {
			return ITableRowRead.empty();
		}

		@Override
		public int add(String key, Object normalizedValue) {
			throw new UnsupportedAsImmutableException(key + "=" + normalizedValue);
		}

		@Override
		public String toString() {
			return "empty";
		}

		@Override
		public int hashCode() {
			return ImmutableList.of().hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			} else if (obj instanceof ITableRowWrite otherRow) {
				return otherRow.size() == 0;
			} else {
				return false;
			}
		}
	};

	static ITableRowRead emptyRead() {
		return EMPTY_ROW_READ;
	}

	static ITableRowWrite emptyRow() {
		return EMPTY_ROW_WRITE;
	}

}
