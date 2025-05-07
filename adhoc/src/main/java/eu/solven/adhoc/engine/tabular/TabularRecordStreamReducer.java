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

import java.util.Collections;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.function.BiConsumer;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.data.tabular.AggregatingColumns;
import eu.solven.adhoc.data.tabular.IMultitypeMergeableGrid;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.map.AdhocMap;
import eu.solven.adhoc.measure.operator.IOperatorsFactory;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IHasGroupBy;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV2;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Value
@Builder
@Slf4j
public class TabularRecordStreamReducer implements ITabularRecordStreamReducer {
	@NonNull
	IOperatorsFactory operatorsFactory;

	@NonNull
	QueryPod queryPod;
	@NonNull
	TableQueryV2 tableQuery;

	protected IMultitypeMergeableGrid<SliceAsMap> makeAggregatingMeasures() {
		return AggregatingColumns.<SliceAsMap>builder().operatorsFactory(operatorsFactory).build();
	}

	@Override
	public IMultitypeMergeableGrid<SliceAsMap> reduce(ITabularRecordStream stream) {
		IMultitypeMergeableGrid<SliceAsMap> grid = makeAggregatingMeasures();

		// TableAggregatesMetadata tableAggregatesMetadata =
		// TableAggregatesMetadata.from(queryPod, tableQuery.getAggregators());

		TabularRecordLogger aggregatedRecordLogger =
				TabularRecordLogger.builder().table(queryPod.getTable().getName()).build();

		// TODO We'd like to log on the last row, to have the number of row actually
		// streamed
		BiConsumer<ITabularRecord, Optional<SliceAsMap>> peekOnCoordinate =
				aggregatedRecordLogger.prepareStreamLogger(tableQuery);

		// Process the underlying stream of data to execute aggregations
		try {
			stream.records()
					// https://stackoverflow.com/questions/25168660/why-is-not-java-util-stream-streamclose-called
					// For any reason, `closeHandler` is not called automatically on a terminal operation
					// .onClose(aggregatedRecordLogger.closeHandler())
					.forEach(input -> forEachRow(input, peekOnCoordinate, grid));

			// https://stackoverflow.com/questions/25168660/why-is-not-java-util-stream-streamclose-called
			aggregatedRecordLogger.closeHandler();
		} catch (RuntimeException e) {
			String msgE = "Issue processing stream from %s".formatted(stream);
			if (e instanceof IllegalStateException illegalStateE) {
				throw new IllegalStateException(msgE, illegalStateE);
			} else {
				throw new RuntimeException(msgE, e);
			}
		}

		return grid;
	}

	protected void forEachRow(ITabularRecord tableRow,
			BiConsumer<ITabularRecord, Optional<SliceAsMap>> peekOnCoordinate,
			IMultitypeMergeableGrid<SliceAsMap> sliceToAgg) {
		Optional<SliceAsMap> optCoordinates = makeCoordinate(queryPod, tableQuery, tableRow);

		peekOnCoordinate.accept(tableRow, optCoordinates);

		if (optCoordinates.isEmpty()) {
			return;
		}

		SliceAsMap coordinates = optCoordinates.get();

		for (FilteredAggregator filteredAggregator : tableQuery.getAggregators()) {
			// We received a pre-aggregated measure
			// DB has seemingly done the aggregation for us
			IValueReceiver valueConsumer = sliceToAgg.contribute(filteredAggregator, coordinates);

			if (queryPod.isDebug()) {
				Object aggregateValue = IValueProvider.getValue(tableRow.onAggregate(filteredAggregator.getAlias()));
				log.info("[DEBUG] Table contributes {}={} -> {}", filteredAggregator, aggregateValue, coordinates);
			}

			if (EmptyAggregation.isEmpty(filteredAggregator.getAggregator())) {
				// TODO Introduce .onBoolean
				valueConsumer.onLong(0);
			} else {
				tableRow.onAggregate(filteredAggregator.getAlias()).acceptConsumer(valueConsumer);
			}
		}
	}

	/**
	 * @param tableQuery
	 * @param tableRow
	 * @return the coordinate for given input, or empty if the input is not compatible with given groupBys.
	 */
	protected Optional<SliceAsMap> makeCoordinate(QueryPod queryPod, IHasGroupBy tableQuery, ITabularRecord tableRow) {
		IAdhocGroupBy groupBy = tableQuery.getGroupBy();
		if (groupBy.isGrandTotal()) {
			return Optional.of(SliceAsMap.fromMap(Collections.emptyMap()));
		}

		NavigableSet<String> groupedByColumns = groupBy.getGroupedByColumns();

		AdhocMap.AdhocMapBuilder coordinatesBuilder = AdhocMap.builder(groupedByColumns);

		for (String groupedByColumn : groupedByColumns) {
			Object value = tableRow.getGroupBy(groupedByColumn);

			if (value == null) {
				if (tableRow.getGroupBys().containsKey(groupedByColumn)) {
					// We received an explicit null
					// Typically happens on a failed LEFT JOIN
					value = valueOnNull(queryPod, groupedByColumn);

					assert value != null : "`null` is not a legal column value";
				} else {
					// The input lack a groupBy coordinate: we exclude it
					// TODO What's a legitimate case leading to this?
					return Optional.empty();
				}
			}

			coordinatesBuilder.append(value);
		}

		AdhocMap asMap = coordinatesBuilder.build();
		return Optional.of(SliceAsMap.fromMap(asMap));
	}

	/**
	 * The value to inject in place of a NULL. Returning a null-reference is not supported.
	 *
	 * @param column
	 *            the column over which a null is encountered. You may customize `null` behavior on a per-column basis.
	 */
	protected Object valueOnNull(QueryPod queryPod, String column) {
		return queryPod.getColumnsManager().onMissingColumn(column);
	}
}
