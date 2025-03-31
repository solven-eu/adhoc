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
package eu.solven.adhoc.data.tabular;

import java.util.Set;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnore;

import eu.solven.adhoc.data.column.IColumnScanner;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.cube.IAdhocQuery;

/**
 * Holds the data for a {@link Set} of {@link IAdhocSlice} and a {@link Set} of {@link IMeasure}. Typical output of an
 * {@link IAdhocQuery} on an {@link eu.solven.adhoc.dag.IAdhocQueryEngine}.
 * 
 * {@link ITabularView} implementations are generally immutable.
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
	long size();

	/**
	 *
	 * @return true if this view is empty.
	 */
	@JsonIgnore
	boolean isEmpty();

	/**
	 *
	 * @return a distinct stream of slices
	 */
	Stream<IAdhocSlice> slices();

	/**
	 * Will apply the rowScanner to each sliced.
	 * 
	 * @param rowScanner
	 */
	void acceptScanner(IColumnScanner<IAdhocSlice> rowScanner);

	/**
	 *
	 * @param rowConverter
	 *            convert each slice and associated values.
	 * @return
	 * @param <U>
	 *            the output type of the rowConvertor
	 */
	<U> Stream<U> stream(ITabularRecordConverter<IAdhocSlice, U> rowConverter);
}
