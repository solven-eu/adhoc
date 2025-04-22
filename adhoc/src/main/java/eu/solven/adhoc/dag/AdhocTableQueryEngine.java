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
package eu.solven.adhoc.dag;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import eu.solven.adhoc.dag.context.ExecutingQueryContext;
import eu.solven.adhoc.dag.observability.SizeAndDuration;
import eu.solven.adhoc.dag.step.AdhocQueryStep;
import eu.solven.adhoc.dag.tabular.ITabularRecordStreamReducer;
import eu.solven.adhoc.dag.tabular.TableQueryToActualTableQuery;
import eu.solven.adhoc.dag.tabular.TabularRecordStreamReducer;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.data.tabular.IMultitypeMergeableGrid;
import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.eventbus.QueryStepIsCompleted;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.StandardOperatorsFactory;
import eu.solven.adhoc.measure.aggregation.collection.UnionSetAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Columnator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.EmptyMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.sum.IAggregationCarrier;
import eu.solven.adhoc.measure.transformator.column_generator.IColumnGenerator;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.groupby.GroupByHelpers;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQuery.TableQueryBuilder;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.util.IAdhocEventBus;
import eu.solven.adhoc.util.IStopwatch;
import eu.solven.adhoc.util.IStopwatchFactory;
import eu.solven.adhoc.util.NotYetImplementedException;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Default {@link IAdhocTableQueryEngine}
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class AdhocTableQueryEngine implements IAdhocTableQueryEngine {

	@NonNull
	@Default
	@Getter
	final IOperatorsFactory operatorsFactory = new StandardOperatorsFactory();

	@NonNull
	@Default
	final IAdhocEventBus eventBus = IAdhocEventBus.BLACK_HOLE;

	@NonNull
	@Default
	IStopwatchFactory stopwatchFactory = () -> {
		Stopwatch stopwatch = Stopwatch.createStarted();

		return stopwatch::elapsed;
	};

	@Override
	public Map<AdhocQueryStep, ISliceToValue> executeTableQueries(ExecutingQueryContext executingQueryContext,
			QueryStepsDag queryStepsDag) {
		Set<TableQuery> tableQueries = prepareForTable(executingQueryContext, queryStepsDag);

		Map<AdhocQueryStep, ISliceToValue> queryStepToValuesOuter =
				executeTableQueries(executingQueryContext, queryStepsDag, tableQueries);

		reportAfterTableQueries(executingQueryContext, queryStepToValuesOuter);

		return queryStepToValuesOuter;
	}

	protected void reportAfterTableQueries(ExecutingQueryContext executingQueryContext,
			Map<AdhocQueryStep, ISliceToValue> queryStepToValuesOuter) {
		if (executingQueryContext.isDebug()) {
			queryStepToValuesOuter.forEach((queryStep, values) -> {
				values.forEachSlice(row -> {
					return rowValue -> {
						eventBus.post(AdhocLogEvent.builder()
								.debug(true)
								.message("%s -> %s".formatted(rowValue, row))
								.source(queryStep)
								.build());
					};
				});

			});
		}
	}

	// Manages concurrency: the logic here should be strictly minimal on-top of concurrency
	protected Map<AdhocQueryStep, ISliceToValue> executeTableQueries(ExecutingQueryContext executingQueryContext,
			ISinkExecutionFeedback queryStepsDag,
			Set<TableQuery> tableQueries) {
		try {
			return executingQueryContext.getFjp().submit(() -> {
				Stream<TableQuery> tableQueriesStream = tableQueries.stream();

				if (executingQueryContext.getOptions().contains(StandardQueryOptions.CONCURRENT)) {
					tableQueriesStream = tableQueriesStream.parallel();
				}
				Map<AdhocQueryStep, ISliceToValue> queryStepToValuesInner = new ConcurrentHashMap<>();
				tableQueriesStream.forEach(tableQuery -> {
					Map<AdhocQueryStep, ISliceToValue> queryStepToValues =
							processOneTableQuery(executingQueryContext, queryStepsDag, tableQuery);

					queryStepToValuesInner.putAll(queryStepToValues);
				});

				return queryStepToValuesInner;
			}).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted", e);
		} catch (ExecutionException e) {
			if (e.getCause() instanceof IllegalStateException) {
				throw new IllegalStateException("Failed", e);
			} else {
				throw new IllegalArgumentException("Failed", e);
			}
		}
	}

	protected Map<AdhocQueryStep, ISliceToValue> processOneTableQuery(ExecutingQueryContext executingQueryContext,
			ISinkExecutionFeedback queryStepsDag,
			TableQuery tableQuery) {
		TableQuery suppressedTableQuery = suppressGeneratedColumns(executingQueryContext, tableQuery);

		// TODO We may be querying multiple times the same suppressedTableQuery
		// e.g. if 2 tableQueries differs only by a suppressedColumn
		TableQueryToActualTableQuery toSuppressed = TableQueryToActualTableQuery.builder()
				.dagTableQuery(tableQuery)
				.suppressedTableQuery(suppressedTableQuery)
				.build();

		IStopwatch stopWatch = stopwatchFactory.createStarted();

		Map<AdhocQueryStep, ISliceToValue> oneQueryStepToValues;
		// Open the stream: the table may or may not return after the actual execution
		try (ITabularRecordStream rowsStream = openTableStream(executingQueryContext, suppressedTableQuery)) {
			oneQueryStepToValues = aggregateStreamToAggregates(executingQueryContext, toSuppressed, rowsStream);
		}

		Duration elapsed = stopWatch.elapsed();

		toSuppressed.getDagTableQuery().getAggregators().forEach(aggregator -> {
			AdhocQueryStep queryStep = AdhocQueryStep.edit(toSuppressed.getDagTableQuery()).measure(aggregator).build();

			ISliceToValue column = oneQueryStepToValues.get(queryStep);

			eventBus.post(QueryStepIsCompleted.builder()
					.querystep(queryStep)
					.nbCells(column.size())
					// The duration is not decomposed per aggregator
					.duration(elapsed)
					.source(AdhocTableQueryEngine.this)
					.build());

			queryStepsDag.registerExecutionFeedback(queryStep,
					SizeAndDuration.builder().size(column.size()).duration(elapsed).build());
		});
		return oneQueryStepToValues;
	}

	/**
	 * @param executingQueryContext
	 * @param dagHolder2
	 * @return the Set of {@link TableQuery} to be executed.
	 */
	protected Set<TableQuery> prepareForTable(ExecutingQueryContext executingQueryContext,
			QueryStepsDag queryStepsDag) {
		// Pack each steps targeting the same groupBy+filters. Multiple measures can be evaluated on such packs.
		Map<MeasurelessQuery, Set<Aggregator>> measurelessToAggregators = new LinkedHashMap<>();

		// https://stackoverflow.com/questions/57134161/how-to-find-roots-and-leaves-set-in-jgrapht-directedacyclicgraph
		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> dag = queryStepsDag.getDag();
		dag.vertexSet().stream().filter(step -> dag.outgoingEdgesOf(step).isEmpty()).forEach(step -> {
			IMeasure leafMeasure = executingQueryContext.resolveIfRef(step.getMeasure());

			if (leafMeasure instanceof Aggregator leafAggregator) {
				MeasurelessQuery measureless = MeasurelessQuery.edit(step).build();

				// We could analyze filters, to swallow a query filtering `k=v` if another query
				// filters `k=v|v2`. This is valid only if they were having the same groupBy, and groupBy includes `k`
				measurelessToAggregators
						.merge(measureless, Collections.singleton(leafAggregator), UnionSetAggregation::unionSet);
			} else if (leafMeasure instanceof EmptyMeasure) {
				// ???
			} else if (leafMeasure instanceof Columnator) {
				// ???
				// Happens if we miss given column
			} else {
				// Happens on Transformator with no underlying measure
				throw new IllegalStateException("Expected simple aggregators. Got %s".formatted(leafMeasure));
			}
		});

		return measurelessToAggregators.entrySet().stream().map(e -> {
			MeasurelessQuery measurelessQuery = e.getKey();
			Set<Aggregator> leafAggregators = e.getValue();
			return TableQuery.edit(measurelessQuery).aggregators(leafAggregators).build();
		}).collect(Collectors.toCollection(LinkedHashSet::new));
	}

	/**
	 * This step handles columns which are not relevant for the tables, but has not been suppressed by the measure tree.
	 * It typically happens when a column is introduced by some {@link Dispatchor} measure, but the actually requested
	 * measure is unrelated (which itself happens when selecting multiple measures, like `many2many+count(*)`).
	 * 
	 * @param executingQueryContext
	 * 
	 * @param tableQuery
	 * @return {@link TableQuery} where calculated columns has been suppressed.
	 */
	protected TableQuery suppressGeneratedColumns(ExecutingQueryContext executingQueryContext, TableQuery tableQuery) {
		// We list the generatedColumns instead of listing the table columns as many tables has lax resolution of
		// columns (e.g. given a joined `tableName.fieldName`, `fieldName` is a valid columnName. `.getColumns` would
		// probably return only one of the 2).
		Set<String> generatedColumns = IColumnGenerator.getColumnGenerators(operatorsFactory,
				// TODO Restrict to the DAG measures
				executingQueryContext.getForest().getMeasures(),
				IValueMatcher.MATCH_ALL)
				.stream()
				.flatMap(cg -> cg.getColumns().keySet().stream())
				.collect(Collectors.toSet());

		Set<String> groupedByCubeColumns =
				tableQuery.getGroupBy().getNameToColumn().keySet().stream().collect(Collectors.toSet());

		TableQueryBuilder edited = TableQuery.edit(tableQuery);

		SetView<String> generatedColumnToSuppressFromGroupBy =
				Sets.intersection(groupedByCubeColumns, generatedColumns);
		if (!generatedColumnToSuppressFromGroupBy.isEmpty()) {
			// All columns has been validated as being generated
			IAdhocGroupBy originalGroupby = tableQuery.getGroupBy();
			IAdhocGroupBy suppressedGroupby =
					GroupByHelpers.suppressColumns(originalGroupby, generatedColumnToSuppressFromGroupBy);
			log.debug("Suppressing generatedColumns in groupBy from {} to {}", originalGroupby, suppressedGroupby);
			edited.groupBy(suppressedGroupby);
		}

		Set<String> filteredCubeColumnsToSuppress =
				FilterHelpers.getFilteredColumns(tableQuery.getFilter()).stream().collect(Collectors.toSet());
		Set<String> generatedColumnToSuppressFromFilter =
				Sets.intersection(filteredCubeColumnsToSuppress, generatedColumns);

		if (!generatedColumnToSuppressFromFilter.isEmpty()) {
			IAdhocFilter originalFilter = tableQuery.getFilter();
			IAdhocFilter suppressedFilter =
					SimpleFilterEditor.suppressColumn(originalFilter, generatedColumnToSuppressFromFilter);
			log.debug("Suppressing generatedColumns in filter from {} to {}", originalFilter, suppressedFilter);
			edited.filter(suppressedFilter);
		}

		return edited.build();
	}

	protected ITabularRecordStream openTableStream(ExecutingQueryContext executingQueryContext, TableQuery tableQuery) {
		return executingQueryContext.getColumnsManager().openTableStream(executingQueryContext, tableQuery);
	}

	protected Map<AdhocQueryStep, ISliceToValue> aggregateStreamToAggregates(
			ExecutingQueryContext executingQueryContext,
			TableQueryToActualTableQuery query,
			ITabularRecordStream stream) {

		IMultitypeMergeableGrid<SliceAsMap> coordinatesToAggregates =
				sinkToAggregates(executingQueryContext, query.getSuppressedTableQuery(), stream);

		return toImmutableChunks(executingQueryContext, query, coordinatesToAggregates);
	}

	protected IMultitypeMergeableGrid<SliceAsMap> sinkToAggregates(ExecutingQueryContext executingQueryContext,
			TableQuery tableQuery,
			ITabularRecordStream stream) {

		ITabularRecordStreamReducer streamReducer =
				makeAggregatedRecordStreamReducer(executingQueryContext, tableQuery);

		return streamReducer.reduce(stream);
	}

	protected ITabularRecordStreamReducer makeAggregatedRecordStreamReducer(ExecutingQueryContext executingQueryContext,
			TableQuery tableQuery) {
		return TabularRecordStreamReducer.builder()
				.operatorsFactory(operatorsFactory)
				.executingQueryContext(executingQueryContext)
				.tableQuery(tableQuery)
				.build();
	}

	protected Optional<Aggregator> isAggregator(Map<String, Set<Aggregator>> columnToAggregators,
			String aggregatorName) {
		return columnToAggregators.values()
				.stream()
				.flatMap(Collection::stream)
				.filter(a -> a.getName().equals(aggregatorName))
				.findAny();
	}

	/**
	 * 
	 * @param executingQueryContext
	 * @param query
	 * @param coordinatesToAggregates
	 * @return a {@link Map} from each {@link Aggregator} to the column of values
	 */
	protected Map<AdhocQueryStep, ISliceToValue> toImmutableChunks(ExecutingQueryContext executingQueryContext,
			TableQueryToActualTableQuery query,
			IMultitypeMergeableGrid<SliceAsMap> coordinatesToAggregates) {
		Map<AdhocQueryStep, ISliceToValue> queryStepToValues = new HashMap<>();
		TableQuery dagTableQuery = query.getDagTableQuery();

		Set<String> suppressedGroupBys = query.getSuppressedGroupBy();

		dagTableQuery.getAggregators().forEach(aggregator -> {
			AdhocQueryStep queryStep = AdhocQueryStep.edit(dagTableQuery).measure(aggregator).build();

			if (executingQueryContext.getOptions().contains(StandardQueryOptions.AGGREGATION_CARRIERS_STAY_WRAPPED)
					&& operatorsFactory.makeAggregation(aggregator) instanceof IAggregationCarrier.IHasCarriers) {
				throw new NotYetImplementedException(
						"Composite+HasCarrier is not yet functional. queryStep=%s".formatted(queryStep));
			}

			// `.closeColumn` is an expensive operation. It induces a delay, e.g. by sorting slices.
			// TODO Sorting is not needed if we do not compute a single transformator with at least 2 different
			// underlyings
			IMultitypeColumnFastGet<SliceAsMap> column = coordinatesToAggregates.closeColumn(aggregator);

			// eventBus.post(QueryStepIsCompleted.builder()
			// .querystep(queryStep)
			// .nbCells(column.size())
			// // TODO How to collect this duration? Especially as the tableQuery computes multiple aggregators at
			// // the same time
			// .duration(Duration.ZERO)
			// .source(this)
			// .build());
			// log.debug("tableQuery={} generated a column with size={}", tableQuery, column.size());

			IMultitypeColumnFastGet<SliceAsMap> columnWithSuppressed;
			if (suppressedGroupBys.isEmpty()) {
				columnWithSuppressed = column;
			} else {
				columnWithSuppressed = restoreSuppressedGroupBy(queryStep, suppressedGroupBys, column);
			}

			// The aggregation step is done: the storage is supposed not to be edited: we
			// re-use it in place, to spare a copy to an immutable container
			queryStepToValues.put(queryStep, SliceToValue.builder().column(columnWithSuppressed).build());
		});
		return queryStepToValues;
	}

	/**
	 * Given a {@link IMultitypeColumnFastGet} from the {@link ITableWrapper}, we may have to add columns which has been
	 * suppressed (e.g. due to IColumnGenerator).
	 * 
	 * Current implementation restore the suppressedColumn by writing a single member. Another project may prefer
	 * writing a constant member (e.g. `suppressed`), or duplicating the value for each possible members of the
	 * suppressed column (through, beware it may lead to a large cartesian product in case of multiple suppressed
	 * columns).
	 * 
	 * @param suppressedColumns
	 * @param aggregator
	 * @param column
	 * @return
	 */
	protected IMultitypeColumnFastGet<SliceAsMap> restoreSuppressedGroupBy(AdhocQueryStep queryStep,
			Set<String> suppressedColumns,
			IMultitypeColumnFastGet<SliceAsMap> column) {
		Map<String, ?> constantValues = valuesForSuppressedColumns(suppressedColumns, queryStep);
		IMultitypeColumnFastGet<SliceAsMap> columnWithSuppressed =
				GroupByHelpers.addConstantColumns(column, constantValues);
		return columnWithSuppressed;
	}

	/**
	 * 
	 * @param suppressedColumns
	 * @param queryStep
	 *            the queryStep can be used to customize the suppress column values
	 * @return
	 */
	protected Map<String, ?> valuesForSuppressedColumns(Set<String> suppressedColumns, AdhocQueryStep queryStep) {
		return suppressedColumns.stream().collect(Collectors.toMap(c -> c, c -> IColumnGenerator.COORDINATE_GENERATED));
	}

}
