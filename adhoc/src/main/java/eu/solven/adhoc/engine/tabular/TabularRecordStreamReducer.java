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
import java.util.stream.IntStream;

import com.google.common.collect.Multimaps;

import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.aggregating.AggregatingColumns;
import eu.solven.adhoc.dataframe.aggregating.AggregatingColumnsDistinct;
import eu.solven.adhoc.dataframe.aggregating.PartitionedMultitypeMergeableGrid;
import eu.solven.adhoc.dataframe.column.partitioned.IPartitioned;
import eu.solven.adhoc.dataframe.column.partitioned.PartitioningHelpers;
import eu.solven.adhoc.dataframe.column.partitioned.ShardingForEachParameters;
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
import eu.solven.adhoc.map.keyset.SequencedSetUnsafe;
import eu.solven.adhoc.measure.model.IAliasedAggregator;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.stream.IConsumingStream;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.pepper.core.PepperStreamHelper;
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

	protected IMultitypeMergeableGrid<ISlice> makeAggregatingMeasures(ITabularRecordStream stream,
			Set<FilteredAggregator> aggregators) {
		Supplier<IMultitypeMergeableGrid<ISlice>> gridFactory;

		if (stream.isDistinctSlices()) {
			gridFactory = () -> AggregatingColumnsDistinct.<ISlice>builder().operatorFactory(operatorFactory).build();
		} else {
			gridFactory = () -> AggregatingColumns.<ISlice>builder().operatorFactory(operatorFactory).build();
		}

		Supplier<IMultitypeMergeableGrid<ISlice>> gridFactory2;
		if (StandardQueryOptions.PARTITIONED.isActive(queryPod.getOptions())) {
			int nbPartitions = AdhocUnsafe.getParallelism();
			if (queryPod.isDebugOrExplain()) {
				log.info("[EXPLAIN] Partitioned is activated with parallelism={}", nbPartitions);
			}

			gridFactory2 = () -> PartitionedMultitypeMergeableGrid.<ISlice, Integer>builder()
					.partitions(IntStream.range(0, nbPartitions).mapToObj(_ -> gridFactory.get()).toList())
					.build();
		} else {
			gridFactory2 = gridFactory;
		}

		if (tableQuery.singleGroupBy().isPresent()) {
			return gridFactory2.get();
		} else {
			return GroupingSetMergeableGrid.builder().gridFactory(gridFactory2).build();
		}
	}

	protected IGroupingSetAnalyzer makeGroupingSetAnalyzer() {
		Optional<IGroupBy> singleGroupBy = tableQuery.singleGroupBy();
		if (singleGroupBy.isPresent()) {
			IGroupBy groupBy = singleGroupBy.get();
			NavigableSet<String> groupedByColumns = groupBy.getSortedColumns();
			SequencedSetLikeList sequencedKeyset = SequencedSetUnsafe.internKeyset(groupedByColumns);
			return UniqueGroupingSetAnalyzer.builder()
					.sequencedKeyset(new GroupByMarker(groupBy, sequencedKeyset))
					.build();
		} else {
			Map<Set<String>, GroupByMarker> columnsToMarker = tableQuery.getGroupBys()
					.stream()
					.collect(PepperStreamHelper.toLinkedMap(IGroupBy::getSortedColumns, gb -> {
						Set<String> groupedByColumns = gb.getSortedColumns();
						SequencedSetLikeList sequencedKeyset = SequencedSetUnsafe.internKeyset(groupedByColumns);
						return new GroupByMarker(gb, sequencedKeyset);
					}));

			return r -> columnsToMarker.get(r.asSlice().columnsKeySet());
		}
	}

	private record GroupByAndTabularRecord(GroupByMarker groupByMarker, ITabularRecord retainedRecord) {
	}

	@SuppressWarnings({ "PMD.AvoidSynchronizedStatement", "PMD.CloseResource", "PMD.UselessQualifiedThis" })
	@Override
	public IMultitypeMergeableGrid<ISlice> reduce(ITabularRecordStream stream) {
		IMultitypeMergeableGrid<ISlice> grid = makeAggregatingMeasures(stream, tableQuery.getAggregators());

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
			try {
				IConsumingStream<GroupByAndTabularRecord> records2 = stream.records().map(input -> {
					GroupByMarker groupByMarker = groupingSetAnalyzer.getGroupingSet(input);

					// Typically useful to discard the column underlying a calculated column
					ITabularRecord retainedRecord = input.retainAll(groupByMarker.keySet().sortedSet());

					return new GroupByAndTabularRecord(groupByMarker, retainedRecord);
				});

				if (grid instanceof IPartitioned<?> partitioned) {
					int nbPartitions = partitioned.getNbPartitions();
					PartitioningHelpers.shardingForEach(ShardingForEachParameters.<GroupByAndTabularRecord>builder()
							.stream(records2)
							.nbPartitions(nbPartitions)
							.partitioner(input -> {
								ISlice slice = input.retainedRecord().asSlice();
								return PartitioningHelpers.getPartitionIndex(slice, nbPartitions);
							})
							.consumer(input -> {
								forEachMeasure(input.groupByMarker(), input.retainedRecord(), peekOnCoordinate, grid);
							})
							.executor(queryPod.getExecutorService())
							.build());
				} else {
					// synchronized: when CONCURRENT is active, Arrow batches may be processed
					// concurrently, so multiple threads can call forEachMeasure simultaneously
					records2.forEach(input -> {
						synchronized (TabularRecordStreamReducer.this) {
							forEachMeasure(input.groupByMarker(), input.retainedRecord(), peekOnCoordinate, grid);
						}
					});
				}
			} finally {
				aggregatedRecordLogger.closeHandler().run();
			}
		} catch (RuntimeException e) {
			if (queryPod.getOptions().contains(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE)) {

				Multimaps.asMap(tableQuery.getGroupByToAggregators()).entrySet().forEach(ee -> {
					IGroupBy groupBy = ee.getKey();
					ISlice errorSlice = AdhocExceptionAsMeasureValueHelper.asSlice(groupBy.getSortedColumns());

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
			ITabularRecord tableRecord,
			BiConsumer<ITabularRecord, ISlice> peekOnCoordinate,
			IMultitypeMergeableGrid<ISlice> sliceToAgg) {
		ISlice slice = tableRecord.asSlice();

		peekOnCoordinate.accept(tableRecord, slice);

		// Thread-safety: when PARTITIONED, forEachPartitioned guarantees each partition is written
		// by a single thread, so no synchronization is needed. When non-partitioned and CONCURRENT,
		// the caller wraps this method in a synchronized block.
		IOpenedSlice openedSlice = sliceToAgg.openSlice(slice);

		for (IAliasedAggregator filteredAggregator : tableQuery.getAggregators(sequencedKeyset.groupBy())) {
			// We received a pre-aggregated measure
			// DB has seemingly done the aggregation for us
			IValueReceiver valueReceiver = openedSlice.contribute(filteredAggregator);

			if (queryPod.isDebug()) {
				Object aggregateValue = IValueProvider.getValue(tableRecord.onAggregate(filteredAggregator.getAlias()));
				log.info("[DEBUG] Table contributes {}={} -> {}", filteredAggregator, aggregateValue, slice);
			}

			if (EmptyAggregation.isEmpty(filteredAggregator.getAggregator())) {
				// TODO Introduce .onBoolean
				valueReceiver.onLong(0);
			} else {
				tableRecord.onAggregate(filteredAggregator.getAlias()).acceptReceiver(valueReceiver);
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
