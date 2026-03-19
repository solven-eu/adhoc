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
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Multimaps;

import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.aggregating.AggregatingColumns;
import eu.solven.adhoc.dataframe.aggregating.AggregatingColumnsDistinct;
import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.dataframe.tabular.IMultitypeMergeableGrid;
import eu.solven.adhoc.dataframe.tabular.IMultitypeMergeableGrid.IOpenedSlice;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.tabular.groupingset.GroupingSetMergeableGrid;
import eu.solven.adhoc.engine.tabular.groupingset.IGroupingSetAnalyzer;
import eu.solven.adhoc.engine.tabular.groupingset.IGroupingSetAnalyzer.GroupByMarker;
import eu.solven.adhoc.engine.tabular.groupingset.UniqueGroupingSetAnalyzer;
import eu.solven.adhoc.exception.AdhocExceptionHelpers;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.map.keyset.SequencedSetLikeList;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV4;
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
	TableQueryV4 tableQuery;

	protected IMultitypeMergeableGrid<ISlice> makeAggregatingMeasures(ITabularRecordStream stream) {

		Supplier<IMultitypeMergeableGrid<ISlice>> gridFactory;

		if (stream.isDistinctSlices()) {
			gridFactory = () -> AggregatingColumnsDistinct.<ISlice>builder().operatorFactory(operatorFactory).build();
		} else {
			gridFactory = () -> AggregatingColumns.<ISlice>builder().operatorFactory(operatorFactory).build();
		}

		if (tableQuery.singleGroupBy().isPresent()) {
			return gridFactory.get();
		} else {
			return GroupingSetMergeableGrid.builder().gridFactory(gridFactory).build();
		}
	}

	protected IGroupingSetAnalyzer makeGroupingSetAnalyzer() {
		Optional<IGroupBy> singleGroupBy = tableQuery.singleGroupBy();
		if (singleGroupBy.isPresent()) {
			IGroupBy groupBy = singleGroupBy.get();
			NavigableSet<String> groupedByColumns = groupBy.getGroupedByColumns();
			SequencedSetLikeList sequencedKeyset = sliceFactory.internKeyset(groupedByColumns);
			return UniqueGroupingSetAnalyzer.builder()
					.sequencedKeyset(new GroupByMarker(groupBy, sequencedKeyset))
					.build();
		} else {
			Map<Set<String>, GroupByMarker> columnsToMarker =
					tableQuery.getGroupBys().stream().collect(Collectors.toMap(IGroupBy::getGroupedByColumns, gb -> {
						Set<String> groupedByColumns = gb.getGroupedByColumns();
						SequencedSetLikeList sequencedKeyset = sliceFactory.internKeyset(groupedByColumns);
						return new GroupByMarker(gb, sequencedKeyset);
					}));

			return r -> columnsToMarker.get(r.asSlice().columnsKeySet());
		}
	}

	@Override
	public IMultitypeMergeableGrid<ISlice> reduce(ITabularRecordStream stream) {
		IMultitypeMergeableGrid<ISlice> grid = makeAggregatingMeasures(stream);

		// Useful to log on the last row, to have the number of row actually streamed
		TabularRecordLogger aggregatedRecordLogger = TabularRecordLogger.builder()
				.table(queryPod.getTable().getName())
				.options(queryPod.getOptions())
				.build();

		BiConsumer<ITabularRecord, ISlice> peekOnCoordinate = aggregatedRecordLogger.prepareStreamLogger(tableQuery);

		IGroupingSetAnalyzer groupingSetAnalyzer = makeGroupingSetAnalyzer();

		// Process the underlying stream of data to execute aggregations
		try {
			// TODO This will consider all measures and all groupBys, while maybe only a subset relates to a relevant
			// CubeQueryStep. This is due to the fact it may be faster to do a single SQL doing a bit too many
			// operations than doing multiple SQLs will seemingly less irrelevant computations, as the multiple SQLs
			// would prevent some sharing. (e.g. Considering DuckDB reading Parquet files on each SQL, it seems
			// reasonable to prefer doing as many computations in a single pass).
			try (Stream<ITabularRecord> records = stream.records().onClose(aggregatedRecordLogger.closeHandler())) {
				records.forEach(input -> {
					GroupByMarker sequencedKeyset = groupingSetAnalyzer.getGroupingSet(input);
					forEachMeasure(sequencedKeyset, input, peekOnCoordinate, grid);
				});
			}

		} catch (RuntimeException e) {
			if (queryPod.getOptions().contains(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE)) {

				Multimaps.asMap(tableQuery.getGroupByToAggregators()).entrySet().forEach(ee -> {
					IGroupBy groupBy = ee.getKey();
					ISlice errorSlice = AdhocExceptionAsMeasureValueHelper.asSlice(groupBy.getGroupedByColumns());

					ee.getValue().forEach(fa -> grid.contribute(errorSlice, fa).onObject(e));
				});

			} else {
				String msgE = "Issue processing stream from %s".formatted(stream);
				throw AdhocExceptionHelpers.wrap(msgE, e);
			}
		}

		// TODO Should we compact the grid? Typically, we may have much less slices that what was initial allocated
		return grid;
	}

	protected void forEachMeasure(GroupByMarker sequencedKeyset,
			ITabularRecord tableRow,
			BiConsumer<ITabularRecord, ISlice> peekOnCoordinate,
			IMultitypeMergeableGrid<ISlice> sliceToAgg) {
		// Typically useful to discard the column underlying a calculated column
		ITabularRecord retainedRecord = tableRow.retainAll(sequencedKeyset.keySet().sortedSet());

		ISlice slice = retainedRecord.asSlice();

		peekOnCoordinate.accept(tableRow, slice);

		IOpenedSlice openedSlice = sliceToAgg.openSlice(slice);

		for (FilteredAggregator filteredAggregator : tableQuery.getAggregators(sequencedKeyset.groupBy())) {
			// We received a pre-aggregated measure
			// DB has seemingly done the aggregation for us
			IValueReceiver valueReceiver = openedSlice.contribute(filteredAggregator);

			if (queryPod.isDebug()) {
				Object aggregateValue = IValueProvider.getValue(tableRow.onAggregate(filteredAggregator.getAlias()));
				log.info("[DEBUG] Table contributes {}={} -> {}", filteredAggregator, aggregateValue, slice);
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
	 * The value to inject in place of a NULL. Returning a null-reference is not supported.
	 *
	 * @param column
	 *            the column over which a null is encountered. You may customize `null` behavior on a per-column basis.
	 */
	protected Object valueOnNull(QueryPod queryPod, String column) {
		return queryPod.getColumnsManager().onMissingColumn(column);
	}
}
