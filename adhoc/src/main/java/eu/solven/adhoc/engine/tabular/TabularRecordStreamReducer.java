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
import java.util.stream.Stream;

import eu.solven.adhoc.collection.AdhocCollectionHelpers;
import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.tabular.AggregatingColumns;
import eu.solven.adhoc.data.tabular.AggregatingColumnsDistinct;
import eu.solven.adhoc.data.tabular.IMultitypeMergeableGrid;
import eu.solven.adhoc.data.tabular.IMultitypeMergeableGrid.IOpenedSlice;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.tabular.groupingset.IGroupingSetAnalyzer;
import eu.solven.adhoc.engine.tabular.groupingset.UniqueGroupingSetAnalyzer;
import eu.solven.adhoc.exception.AdhocExceptionHelpers;
import eu.solven.adhoc.map.SliceHelpers;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.map.keyset.SequencedSetLikeList;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.adhoc.util.NotYetImplementedException;
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
	TableQueryV3 tableQuery;

	protected IMultitypeMergeableGrid<IAdhocSlice> makeAggregatingMeasures(ITabularRecordStream stream) {
		if (stream.isDistinctSlices()) {
			return AggregatingColumnsDistinct.<IAdhocSlice>builder().operatorFactory(operatorFactory).build();
		} else {
			return AggregatingColumns.<IAdhocSlice>builder().operatorFactory(operatorFactory).build();
		}
	}

	protected IGroupingSetAnalyzer makeGroupingSetAnalyzer() {
		IGroupBy singleGroupBy;
		if (tableQuery.getGroupBys().isEmpty()) {
			singleGroupBy = IGroupBy.GRAND_TOTAL;
		} else if (tableQuery.getGroupBys().size() == 1) {
			singleGroupBy = AdhocCollectionHelpers.getFirst(tableQuery.getGroupBys());
		} else {
			throw new NotYetImplementedException("GROUPING SET are not supported yet");
		}
		NavigableSet<String> groupedByColumns = singleGroupBy.getGroupedByColumns();
		SequencedSetLikeList sequencedKeyset = queryPod.getSliceFactory().internKeyset(groupedByColumns);
		return UniqueGroupingSetAnalyzer.builder().sequencedKeyset(sequencedKeyset).build();
	}

	@Override
	public IMultitypeMergeableGrid<IAdhocSlice> reduce(ITabularRecordStream stream) {
		IMultitypeMergeableGrid<IAdhocSlice> grid = makeAggregatingMeasures(stream);

		// Useful to log on the last row, to have the number of row actually streamed
		TabularRecordLogger aggregatedRecordLogger = TabularRecordLogger.builder()
				.table(queryPod.getTable().getName())
				.options(queryPod.getOptions())
				.build();

		BiConsumer<ITabularRecord, IAdhocSlice> peekOnCoordinate =
				aggregatedRecordLogger.prepareStreamLogger(tableQuery);

		IGroupingSetAnalyzer groupingSetAnalyzer = makeGroupingSetAnalyzer();

		// Process the underlying stream of data to execute aggregations
		try {
			// https://stackoverflow.com/questions/25168660/why-is-not-java-util-stream-streamclose-called
			// For any reason, `closeHandler` is not called automatically on a terminal
			// operation
			try (Stream<ITabularRecord> records = stream.records().onClose(aggregatedRecordLogger.closeHandler())) {
				records.forEach(input -> {
					SequencedSetLikeList sequencedKeyset = groupingSetAnalyzer.getGroupingSet(input);
					forEachMeasure(sequencedKeyset, input, peekOnCoordinate, grid);
				});
			}

		} catch (RuntimeException e) {
			if (queryPod.getOptions().contains(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE)) {

				tableQuery.getGroupBys().forEach(groupBy -> {
					IAdhocSlice errorSlice = AdhocExceptionAsMeasureValueHelper.asSlice(groupBy.getGroupedByColumns());

					tableQuery.getAggregators().forEach(fa -> grid.contribute(errorSlice, fa).onObject(e));
				});

			} else {
				String msgE = "Issue processing stream from %s".formatted(stream);
				throw AdhocExceptionHelpers.wrap(msgE, e);
			}
		}

		// TODO Should we compact the grid? Typically, we may have much less slices that what was initial allocated
		return grid;
	}

	protected void forEachMeasure(SequencedSetLikeList sequencedKeyset,
			ITabularRecord tableRow,
			BiConsumer<ITabularRecord, IAdhocSlice> peekOnCoordinate,
			IMultitypeMergeableGrid<IAdhocSlice> sliceToAgg) {
		IAdhocSlice coordinates = makeCoordinate(sequencedKeyset, tableRow);

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
	 * @param groupBy
	 * @param tableRecord
	 * @return the coordinate for given input.
	 */
	protected IAdhocSlice makeCoordinate(SequencedSetLikeList groupBy, ITabularRecord tableRecord) {
		if (groupBy.isEmpty()) {
			return SliceHelpers.grandTotal();
		}

		// BEWARE This order may differ from tableSlice due to calculatedColumns
		// NavigableSet<String> groupedByColumns = groupBy.getGroupedByColumns();

		if (
		// groupBy.asList().equals(tableSlice.columnsKeySet()) Iterables.elementsEqual(tableSlice.columnsKeySet(),
		// groupedByColumns)
		groupBy.size() == tableRecord.columnsKeySet().size()) {
			// BEWARE Could we have same size but different columns?
			// In most cases, the tableSlice should have same columns as requested by the groupBy
			return tableRecord.getGroupBys();
		} else {
			// In some edge-cases (like calculatedColumns, or InMemoryTable), we may receive more columns than expected,
			// or in a different order (What would be the impact of different order else still relevant columns?).
			return tableRecord.getGroupBys().retainAll(groupBy.sortedSet());
		}
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
