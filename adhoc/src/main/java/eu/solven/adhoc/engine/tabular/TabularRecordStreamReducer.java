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

import java.util.NavigableSet;
import java.util.function.BiConsumer;

import com.google.common.collect.Iterables;

import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.data.tabular.AggregatingColumns;
import eu.solven.adhoc.data.tabular.AggregatingColumnsDistinct;
import eu.solven.adhoc.data.tabular.IMultitypeMergeableGrid;
import eu.solven.adhoc.data.tabular.IMultitypeMergeableGrid.IOpenedSlice;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.exception.AdhocExceptionHelpers;
import eu.solven.adhoc.map.factory.IBuildableIntoMap;
import eu.solven.adhoc.map.factory.IMapBuilderThroughKeys;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IHasGroupBy;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV2;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Standard {@link ITabularRecordStreamReducer}.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
@Slf4j
public class TabularRecordStreamReducer implements ITabularRecordStreamReducer {
	@NonNull
	IOperatorFactory operatorFactory;

	@NonNull
	ISliceFactory sliceFactory;

	@NonNull
	QueryPod queryPod;
	@NonNull
	TableQueryV2 tableQuery;

	protected IMultitypeMergeableGrid<IAdhocSlice> makeAggregatingMeasures(ITabularRecordStream stream) {
		if (stream.isDistinctSlices()) {
			return AggregatingColumnsDistinct.<IAdhocSlice>builder().operatorFactory(operatorFactory).build();
		} else {
			return AggregatingColumns.<IAdhocSlice>builder().operatorFactory(operatorFactory).build();
		}
	}

	@Override
	public IMultitypeMergeableGrid<IAdhocSlice> reduce(ITabularRecordStream stream) {
		IMultitypeMergeableGrid<IAdhocSlice> grid = makeAggregatingMeasures(stream);

		TabularRecordLogger aggregatedRecordLogger =
				TabularRecordLogger.builder().table(queryPod.getTable().getName()).build();

		// TODO We'd like to log on the last row, to have the number of row actually
		// streamed
		BiConsumer<ITabularRecord, IAdhocSlice> peekOnCoordinate =
				aggregatedRecordLogger.prepareStreamLogger(tableQuery);

		// Process the underlying stream of data to execute aggregations
		try {
			stream.records()
					// https://stackoverflow.com/questions/25168660/why-is-not-java-util-stream-streamclose-called
					// For any reason, `closeHandler` is not called automatically on a terminal
					// operation
					// .onClose(aggregatedRecordLogger.closeHandler())
					.forEach(input -> forEachRow(input, peekOnCoordinate, grid));

			// https://stackoverflow.com/questions/25168660/why-is-not-java-util-stream-streamclose-called
			aggregatedRecordLogger.closeHandler();
		} catch (RuntimeException e) {
			if (queryPod.getOptions().contains(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE)) {
				NavigableSet<String> groupedByColumns = tableQuery.getGroupBy().getGroupedByColumns();
				IAdhocSlice errorSlice = AdhocExceptionAsMeasureValueHelper.asSlice(groupedByColumns);

				tableQuery.getAggregators().forEach(fa -> grid.contribute(errorSlice, fa).onObject(e));
			} else {
				String msgE = "Issue processing stream from %s".formatted(stream);
				throw AdhocExceptionHelpers.wrap(e, msgE);
			}
		}

		return grid;
	}

	protected void forEachRow(ITabularRecord tableRow,
			BiConsumer<ITabularRecord, IAdhocSlice> peekOnCoordinate,
			IMultitypeMergeableGrid<IAdhocSlice> sliceToAgg) {
		IAdhocSlice coordinates = makeCoordinate(queryPod, tableQuery, tableRow);

		peekOnCoordinate.accept(tableRow, coordinates);

		IOpenedSlice openedSlice = sliceToAgg.openSlice(coordinates);

		for (FilteredAggregator filteredAggregator : tableQuery.getAggregators()) {
			// We received a pre-aggregated measure
			// DB has seemingly done the aggregation for us
			IValueReceiver valueReceiver = openedSlice.contribute(filteredAggregator);

			if (queryPod.isDebug()) {
				Object aggregateValue = IValueProvider.getValue(tableRow.onAggregate(filteredAggregator.getAlias()));
				log.info("[DEBUG] Table contributes {}={} -> {}", filteredAggregator, aggregateValue, coordinates);
			}

			if (EmptyAggregation.isEmpty(filteredAggregator.getAggregator())) {
				// TODO Introduce .onBoolean
				valueReceiver.onLong(0);
			} else {
				tableRow.onAggregate(filteredAggregator.getAlias()).acceptReceiver(valueReceiver);
			}
		}
	}

	/**
	 * @param tableQuery
	 * @param tableSlice
	 * @return the coordinate for given input.
	 */
	protected IAdhocSlice makeCoordinate(QueryPod queryPod, IHasGroupBy tableQuery, ITabularRecord tableSlice) {
		IAdhocGroupBy groupBy = tableQuery.getGroupBy();
		if (groupBy.isGrandTotal()) {
			return SliceAsMap.grandTotal();
		}

		// BEWARE This order may differ from tableSlice due to calculatedColumns
		NavigableSet<String> groupedByColumns = groupBy.getGroupedByColumns();

		IBuildableIntoMap buildableIntoMap;
		if (Iterables.elementsEqual(tableSlice.columnsKeySet(), groupedByColumns)) {
			// In most cases, the tableSlice should have same columns as requested by the groupBy
			return tableSlice.getGroupBys();
		} else {
			// In some edge-cases (like calculatedColumns, or InMemoryTable), we may receive more columns than expected,
			// or in a different order.
			IMapBuilderThroughKeys coordinatesBuilder = sliceFactory.newMapBuilder();

			// `forEachGroupBy` enables not doing many individual `.get`
			tableSlice.forEachGroupBy((sliceColumn, value) -> {
				if (!groupedByColumns.contains(sliceColumn)) {
					return;
				}

				if (value == null) {
					// We received an explicit null
					// Typically happens on a failed LEFT JOIN
					value = valueOnNull(queryPod, sliceColumn);

					assert value != null : "`null` is not a legal column value";
				}

				coordinatesBuilder.put(sliceColumn, value);
			});
			buildableIntoMap = coordinatesBuilder;
		}

		return buildableIntoMap.build().asSlice();
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
