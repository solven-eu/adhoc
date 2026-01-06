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
package eu.solven.adhoc.dictionary.page;

import eu.solven.adhoc.util.immutable.UnsupportedAsImmutableException;

/**
 * Represents a row in a table.
 * 
 * @author Benoit Lacelle
 */
public interface ITableRow {

	int size();

	int add(String key, Object normalizedValue);

	static ITableRow empty() {
		return new ITableRow() {

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
				throw new UnsupportedAsImmutableException();
			}
		};
	}

	/**
	 * Mark this as read-only.
	 */
	ITableRowRead freeze();

}
