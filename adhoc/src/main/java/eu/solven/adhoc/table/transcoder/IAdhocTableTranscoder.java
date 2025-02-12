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
package eu.solven.adhoc.table.transcoder;

import java.util.Optional;

import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.table.IAdhocTableWrapper;

/**
 * Holds the logic mapping from the columns names in {@link IAdhocQuery} and columnNames in {@link IAdhocTableWrapper}.
 * <p>
 * This enables re-using a {@link eu.solven.adhoc.measure.AdhocMeasureBag} for different {@link IAdhocTableWrapper}.
 *
 * @see TranscodingContext
 * @see IAdhocTableReverseTranscoder
 *
 */
public interface IAdhocTableTranscoder {
	/**
	 * This may return null for convenience of implementation.
	 *
	 * @param queried
	 *            a column name typically used by an {@link IAdhocQuery}.
	 * @return the equivalent underlying column name, typically used by the database. If null, it means the column maps
	 *         to itself.
	 */
	String underlying(String queried);

	/**
	 * This never returns null for convenience of callers.
	 *
	 * @param queried
	 *            a column name typically used by an {@link IAdhocQuery}.
	 * @return the equivalent underlying column name, typically used by the database.
	 */
	default String underlyingNonNull(String queried) {
		String underlyingColumn = underlying(queried);

		return Optional.ofNullable(underlyingColumn).orElse(queried);
	}
}
