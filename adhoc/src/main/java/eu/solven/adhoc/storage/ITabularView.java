/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.storage;

import java.util.stream.Stream;

import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.slice.IAdhocSlice;

/**
 * Storage for static data (i.e. not mutating data). Typical output of an {@link IAdhocQuery}.
 * 
 * @author Benoit Lacelle
 *
 */
public interface ITabularView {

	/**
	 * 
	 * @return an empty and immutable {@link ITabularView}
	 */
	static ITabularView empty() {
		return MapBasedTabularView.empty();
	}

	/**
	 * 
	 * @return the number of slices in this view
	 */
	int size();

	/**
	 * 
	 * @return true if this view is empty.
	 */
	boolean isEmpty();

	/**
	 * 
	 * @return a distinct stream of slices
	 */
	Stream<IAdhocSlice> slices();

	void acceptScanner(IRowScanner<IAdhocSlice> rowScanner);

	<U> Stream<U> stream(IRowConverter<IAdhocSlice, U> rowScanner);
}
