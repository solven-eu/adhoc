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
package eu.solven.adhoc.data.row;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import eu.solven.adhoc.query.table.TableQuery;

/**
 * Holds a resource to a stream of data, typically given a {@link TableQuery}.
 * 
 * @author Benoit Lacelle
 */
public interface ITabularRecordStream extends AutoCloseable {
	// Object getNullPlaceholder();

	/**
	 * BEWARE We do not rely on Stream caracteristics as a {@link Stream} may be distinct (with `.distinct()`) due to
	 * Aggregates but having equals slices.
	 * 
	 * @return true if the output {@link ITabularRecord} is guaranteed to refer to distinct slices.
	 */
	boolean isDistinctSlices();

	/**
	 * Only the first call is guaranteed (by interface contract) to succeed.
	 * 
	 * @return a {@link Stream} of {@link ITabularRecord}
	 */
	Stream<ITabularRecord> records();

	/**
	 * @deprecated Used for unitTests
	 * @return the stream collected into a List
	 */
	@Deprecated
	default List<Map<String, ?>> toList() {
		return records().<Map<String, ?>>map(ITabularRecord::asMap).toList();
	}

	// Force not to throw an explicit Exception
	@Override
	void close();

	static ITabularRecordStream empty() {
		return new ITabularRecordStream() {

			@Override
			public Stream<ITabularRecord> records() {
				return Stream.empty();
			}

			@Override
			public boolean isDistinctSlices() {
				return true;
			}

			@Override
			public void close() {

			}
		};
	}
}
