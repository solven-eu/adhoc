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

import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.cuboid.slice.SliceHelpers;
import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.dataframe.row.TabularRecordOverMaps;
import eu.solven.adhoc.dataframe.row.TabularRecordOverMaps.TabularRecordOverMapsBuilder;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.stream.ConsumingStream;
import eu.solven.adhoc.stream.IConsumingStream;
import lombok.experimental.UtilityClass;

/**
 * Helps implementing {@link StandardQueryOptions#EXCEPTIONS_AS_MEASURE_VALUE}
 *
 * @author Benoit Lacelle
 */
@UtilityClass
public class AdhocExceptionAsMeasureValueHelper {

	public static ITabularRecordStream makeErrorStream(TableQueryV4 transcodedQuery, Throwable e) {
		return new ITabularRecordStream() {

			protected Stream<ITabularRecord> recordsAsStream() {
				TabularRecordOverMapsBuilder errorRecordBuilder = TabularRecordOverMaps.builder();

				return transcodedQuery.getGroupByToAggregators().asMap().entrySet().stream().map(e -> {
					IGroupBy groupBy = e.getKey();
					NavigableSet<String> groupedByColumns = groupBy.getSortedColumns();

					errorRecordBuilder.slice(groupBy, asSlice(groupedByColumns));
					e.getValue().forEach(fa -> errorRecordBuilder.aggregate(fa.getAlias(), e));

					return errorRecordBuilder.build();
				});

			}

			@Override
			public IConsumingStream<ITabularRecord> records() {
				return ConsumingStream.<ITabularRecord>builder()
						.source(consumer -> recordsAsStream().forEach(consumer))
						.build();
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

	public static ISlice asSlice(NavigableSet<String> columns) {
		return SliceHelpers.asSlice(asMap(columns));
	}

	public static Map<String, ?> asMap(NavigableSet<String> columns) {
		Map<String, Object> errorSliceAsMap = new TreeMap<>();
		columns.forEach(c -> errorSliceAsMap.put(c, "error"));
		return errorSliceAsMap;
	}
}
