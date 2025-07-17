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
package eu.solven.adhoc.engine.tabular;

import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.stream.Stream;

import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.TabularRecordOverMaps;
import eu.solven.adhoc.data.row.TabularRecordOverMaps.TabularRecordOverMapsBuilder;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.table.TableQueryV2;
import lombok.experimental.UtilityClass;

/**
 * Helps implementing {@link StandardQueryOptions#EXCEPTIONS_AS_MEASURE_VALUEX}
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class AdhocExceptionAsMeasureValueHelper {

	public static Map<String, ?> asMap(NavigableSet<String> columns) {
		Map<String, Object> errorSliceAsMap = new TreeMap<>();
		columns.forEach(c -> errorSliceAsMap.put(c, "error"));
		return errorSliceAsMap;
	}

	public static SliceAsMap asSlice(NavigableSet<String> columns) {
		return SliceAsMap.fromMap(asMap(columns));
	}

	public static ITabularRecordStream makeErrorStream(TableQueryV2 transcodedQuery, Throwable e) {
		return new ITabularRecordStream() {

			@Override
			public Stream<ITabularRecord> records() {
				TabularRecordOverMapsBuilder errorRecordBuilder = TabularRecordOverMaps.builder();

				NavigableSet<String> groupedByColumns = transcodedQuery.getGroupBy().getGroupedByColumns();
				Map<String, ?> errorSlice = asMap(groupedByColumns);

				errorRecordBuilder.slice(SliceAsMap.fromMap(errorSlice));
				transcodedQuery.getAggregators().forEach(fa -> errorRecordBuilder.aggregate(fa.getAlias(), e));

				ITabularRecord errorRecord = errorRecordBuilder.build();
				return Stream.of(errorRecord);
			}

			@Override
			public boolean isDistinctSlices() {
				// Single error slice, hence distinct==true
				return true;
			}

			@Override
			public void close() {
				// nothing to close
			}
		};
	}
}
