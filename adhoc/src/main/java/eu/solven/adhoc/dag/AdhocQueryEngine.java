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
package eu.solven.adhoc.dag;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

import com.google.common.base.Stopwatch;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.dag.step.AdhocQueryStep;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.column.IColumnScanner;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.data.tabular.IMultitypeMergeableGrid;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.eventbus.AdhocQueryPhaseIsCompleted;
import eu.solven.adhoc.eventbus.QueryLifecycleEvent;
import eu.solven.adhoc.eventbus.QueryStepIsCompleted;
import eu.solven.adhoc.eventbus.QueryStepIsEvaluating;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.IHasOperatorsFactory;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.StandardOperatorsFactory;
import eu.solven.adhoc.measure.aggregation.collection.UnionSetAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Columnator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.EmptyMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.measure.sum.IAggregationCarrier;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.ITransformator;
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
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * The default query-engine.
 *
 * @author Benoit Lacelle
 */
@Builder(toBuilder = true)
@Slf4j
public class AdhocQueryEngine implements IAdhocQueryEngine, IHasOperatorsFactory {
	private final UUID engineId = UUID.randomUUID();

	// This shall not conflict with any user measure
	private final String emptyMeasureName = "$ADHOC$empty-" + engineId;

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
	public String toString() {
		return this.getClass().getSimpleName() + "=" + engineId;
	}

	@Override
	public ITabularView execute(ExecutingQueryContext executingQueryContext) {
		IStopwatch stopWatch = stopwatchFactory.createStarted();
		boolean postedAboutDone = false;
		try {
			postAboutQueryStart(executingQueryContext);

			QueryStepsDag queryStepsDag = makeQueryStepsDag(executingQueryContext);

			if (executingQueryContext.isExplain() || executingQueryContext.isDebug()) {
				explainDagSteps(queryStepsDag);
			}

			ITabularView tabularView = executeDag(executingQueryContext, queryStepsDag);

			if (executingQueryContext.isExplain() || executingQueryContext.isDebug()) {
				explainDagPerfs(queryStepsDag);
			}

			postAboutQueryDone(executingQueryContext, "OK", stopWatch);
			postedAboutDone = true;
			return tabularView;
		} catch (RuntimeException e) {
			// TODO Add the Exception to the event
			postAboutQueryDone(executingQueryContext, "KO", stopWatch);
			postedAboutDone = true;

			String eMsg = "Issue executing query=%s options=%s".formatted(executingQueryContext.getQuery(),
					executingQueryContext.getOptions());

			if (e instanceof IllegalStateException illegalStateE) {
				throw new IllegalStateException(eMsg, illegalStateE);
			} else {
				throw new IllegalArgumentException(eMsg, e);
			}
		} finally {
			if (!postedAboutDone) {
				// This may happen in case of OutOfMemoryError, or any uncaught exception
				postAboutQueryDone(executingQueryContext, "KO_Uncaught", stopWatch);
			}
		}
	}

	protected void postAboutQueryStart(ExecutingQueryContext executingQueryContext) {
		eventBus.post(AdhocLogEvent.builder()
				.message("Executing on table=%s measures=%s query=%s".formatted(executingQueryContext.getTable()
						.getName(), executingQueryContext.getForest().getName(), executingQueryContext.getQuery()))
				.source(this)
				.tag(AdhocQueryMonitor.TAG_QUERY_LIFECYCLE)
				.tag(AdhocQueryMonitor.TAG_QUERY_START)
				.build());

		eventBus.post(QueryLifecycleEvent.builder()
				.query(executingQueryContext)
				.tag(AdhocQueryMonitor.TAG_QUERY_LIFECYCLE)
				.tag(AdhocQueryMonitor.TAG_QUERY_START)
				.build());
	}

	protected void postAboutQueryDone(ExecutingQueryContext executingQueryContext,
			String status,
			IStopwatch stopWatch) {
		eventBus.post(AdhocLogEvent.builder()
				.message("Executed status=%s duration=%s on table=%s measures=%s query=%s".formatted(status,
						stopWatch.elapsed(),
						executingQueryContext.getTable().getName(),
						executingQueryContext.getForest().getName(),
						executingQueryContext.getQuery()))
				.source(this)
				.performance(true)
				.tag(AdhocQueryMonitor.TAG_QUERY_LIFECYCLE)
				.tag(AdhocQueryMonitor.TAG_QUERY_DONE)
				.build());

		eventBus.post(QueryLifecycleEvent.builder()
				.query(executingQueryContext)
				.tag(AdhocQueryMonitor.TAG_QUERY_LIFECYCLE)
				.tag(AdhocQueryMonitor.TAG_QUERY_DONE)
				.build());
	}

	protected void explainDagSteps(QueryStepsDag queryStepsDag) {
		makeDagExplainer().explain(queryStepsDag);
	}

	protected void explainDagPerfs(QueryStepsDag queryStepsDag) {
		makeDagExplainerForPerfs().explain(queryStepsDag);
	}

	protected DagExplainer makeDagExplainer() {
		return DagExplainer.builder().eventBus(eventBus).build();
	}

	protected DagExplainerForPerfs makeDagExplainerForPerfs() {
		return DagExplainerForPerfs.builder().eventBus(eventBus).build();
	}

	protected QueryStepsDag makeQueryStepsDag(ExecutingQueryContext executingQueryContext) {
		QueryStepsDagBuilder queryStepsDagBuilder = makeQueryStepsDagsBuilder(executingQueryContext);

		// Add explicitly requested steps
		Set<IMeasure> queriedMeasures = convertToQueriedSteps(executingQueryContext);
		queriedMeasures.forEach(queryStepsDagBuilder::addRoot);

		// Add implicitly requested steps
		while (queryStepsDagBuilder.hasLeftovers()) {
			AdhocQueryStep queryStep = queryStepsDagBuilder.pollLeftover();

			IMeasure measure = executingQueryContext.resolveIfRef(queryStep.getMeasure());

			if (measure instanceof Aggregator aggregator) {
				log.debug("Aggregators (here {}) do not have any underlying measure", aggregator);
			} else if (measure instanceof IHasUnderlyingMeasures measureWithUnderlyings) {
				ITransformator wrappedQueryStep = measureWithUnderlyings.wrapNode(operatorsFactory, queryStep);

				List<AdhocQueryStep> underlyingSteps;
				try {
					underlyingSteps = wrappedQueryStep.getUnderlyingSteps().stream().map(underlyingStep -> {
						// Make sure the DAG has actual measure nodes, and not references
						IMeasure notRefMeasure = executingQueryContext.resolveIfRef(underlyingStep.getMeasure());
						return AdhocQueryStep.edit(underlyingStep).measure(notRefMeasure).build();
					}).toList();
				} catch (RuntimeException e) {
					throw new IllegalStateException(
							"Issue computing the underlying querySteps for %s".formatted(queryStep),
							e);
				}

				queryStepsDagBuilder.registerUnderlyings(queryStep, underlyingSteps);
			} else {
				throw new UnsupportedOperationException(PepperLogHelper.getObjectAndClass(measure).toString());
			}
		}

		queryStepsDagBuilder.sanityChecks();

		return queryStepsDagBuilder.getQueryDag();
	}

	protected Set<IMeasure> convertToQueriedSteps(ExecutingQueryContext executingQueryContext) {
		Set<IMeasure> measures = executingQueryContext.getQuery().getMeasures();
		Set<IMeasure> queriedMeasures;
		if (measures.isEmpty()) {
			IMeasure defaultMeasure = defaultMeasure();
			queriedMeasures = Set.of(defaultMeasure);
		} else {
			queriedMeasures =
					measures.stream().map(ref -> executingQueryContext.resolveIfRef(ref)).collect(Collectors.toSet());
		}
		return queriedMeasures;
	}

	/**
	 * This measure is used to materialize slices. Typically used to list coordinates along a column.
	 * 
	 * @return the measure to be considered if not measure is provided to the query
	 */
	protected IMeasure defaultMeasure() {
		return Aggregator.builder().name(emptyMeasureName).aggregationKey(EmptyAggregation.KEY).build();
	}

	protected QueryStepsDagBuilder makeQueryStepsDagsBuilder(ExecutingQueryContext executingQueryContext) {
		return new QueryStepsDagBuilder(executingQueryContext.getTable().getName(), executingQueryContext.getQuery());
	}

	protected ITabularRecordStream openTableStream(ExecutingQueryContext executingQueryContext, TableQuery tableQuery) {
		return executingQueryContext.getColumnsManager().openTableStream(executingQueryContext, tableQuery);
	}

	protected ITabularView executeDag(ExecutingQueryContext executingQueryContext, QueryStepsDag queryStepsDag) {
		Map<TableQueryToActualTableQuery, ITabularRecordStream> tableQueryToStream =
				openTableStreams(executingQueryContext, queryStepsDag);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("opening").source(this).build());

		Map<AdhocQueryStep, ISliceToValue> queryStepToValues =
				preAggregate(executingQueryContext, queryStepsDag, tableQueryToStream);

		// We're done with the input stream: the DB can be shutdown, we could answer the
		// query
		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("aggregate").source(this).build());

		walkDagUpToQueriedMeasures(executingQueryContext, queryStepsDag, queryStepToValues);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("transform").source(this).build());

		ITabularView tabularView = toTabularView(executingQueryContext, queryStepsDag, queryStepToValues);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("view").source(this).build());

		return tabularView;
	}

	protected Map<TableQueryToActualTableQuery, ITabularRecordStream> openTableStreams(
			ExecutingQueryContext executingQueryContext,
			QueryStepsDag queryStepsDag) {
		Set<TableQuery> tableQueries = prepareForTable(executingQueryContext, queryStepsDag);

		Map<TableQueryToActualTableQuery, ITabularRecordStream> tableQueryToStream = new HashMap<>();
		for (TableQuery tableQuery : tableQueries) {
			TableQuery suppressedTableQuery = suppressGeneratedColumns(executingQueryContext, tableQuery);

			// TODO We may be querying multiple times the same suppressedTableQuery
			TableQueryToActualTableQuery toSuppressed = TableQueryToActualTableQuery.builder()
					.dagTableQuery(tableQuery)
					.suppressedTableQuery(suppressedTableQuery)
					.build();

			ITabularRecordStream rowsStream = openTableStream(executingQueryContext, suppressedTableQuery);
			tableQueryToStream.put(toSuppressed, rowsStream);
		}
		return tableQueryToStream;
	}

	protected Map<AdhocQueryStep, ISliceToValue> preAggregate(ExecutingQueryContext executingQueryContext,
			QueryStepsDag queryStepsDag,
			Map<TableQueryToActualTableQuery, ITabularRecordStream> tableToRowsStream) {
		SetMultimap<String, Aggregator> columnToAggregators = columnToAggregators(executingQueryContext, queryStepsDag);

		Map<AdhocQueryStep, ISliceToValue> queryStepToValues = new LinkedHashMap<>();

		// This is the only step consuming the input stream
		tableToRowsStream.forEach((tableQuery, rowsStream) -> {
			IStopwatch stopWatch = stopwatchFactory.createStarted();

			Map<AdhocQueryStep, ISliceToValue> oneQueryStepToValues =
					aggregateStreamToAggregates(executingQueryContext, tableQuery, rowsStream, columnToAggregators);

			Duration elapsed = stopWatch.elapsed();

			tableQuery.getDagTableQuery().getAggregators().forEach(aggregator -> {
				AdhocQueryStep queryStep =
						AdhocQueryStep.edit(tableQuery.getDagTableQuery()).measure(aggregator).build();

				ISliceToValue column = oneQueryStepToValues.get(queryStep);

				eventBus.post(QueryStepIsCompleted.builder()
						.querystep(queryStep)
						.nbCells(column.size())
						// The duration is not decomposed per aggregator
						.duration(elapsed)
						.source(AdhocQueryEngine.this)
						.build());

				queryStepsDag.registerExecutionFeedback(queryStep,
						SizeAndDuration.builder().size(column.size()).duration(elapsed).build());
			});

			queryStepToValues.putAll(oneQueryStepToValues);
		});

		if (executingQueryContext.isDebug()) {
			queryStepToValues.forEach((aggregateStep, values) -> {
				values.forEachSlice(row -> {
					return rowValue -> {
						eventBus.post(AdhocLogEvent.builder()
								.debug(true)
								.message("%s -> %s".formatted(rowValue, row))
								.source(aggregateStep)
								.build());
					};
				});

			});
		}
		return queryStepToValues;
	}

	protected SetMultimap<String, Aggregator> columnToAggregators(ExecutingQueryContext executingQueryContext,
			QueryStepsDag dagHolder) {
		SetMultimap<String, Aggregator> columnToAggregators =
				MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();

		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> dag = dagHolder.getDag();
		dag.vertexSet()
				.stream()
				.filter(step -> dag.outDegreeOf(step) == 0)
				.map(AdhocQueryStep::getMeasure)
				.forEach(measure -> {
					measure = executingQueryContext.resolveIfRef(measure);

					if (measure instanceof Aggregator a) {
						columnToAggregators.put(a.getColumnName(), a);
					} else if (measure instanceof EmptyMeasure) {
						// ???
					} else if (measure instanceof Columnator) {
						// ???
						// Happens if we miss given column
					} else {
						throw new UnsupportedOperationException(
								"%s".formatted(PepperLogHelper.getObjectAndClass(measure)));
					}
				});
		return columnToAggregators;
	}

	protected Map<AdhocQueryStep, ISliceToValue> aggregateStreamToAggregates(
			ExecutingQueryContext executingQueryContext,
			TableQueryToActualTableQuery query,
			ITabularRecordStream stream,
			SetMultimap<String, Aggregator> columnToAggregators) {

		IMultitypeMergeableGrid<SliceAsMap> coordinatesToAggregates =
				sinkToAggregates(executingQueryContext, query.getSuppressedTableQuery(), stream, columnToAggregators);

		return toImmutableChunks(executingQueryContext, query, coordinatesToAggregates);
	}

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

	protected Map<String, ?> valuesForSuppressedColumns(Set<String> suppressedColumns, AdhocQueryStep queryStep) {
		return suppressedColumns.stream().collect(Collectors.toMap(c -> c, c -> IColumnGenerator.COORDINATE_GENERATED));
	}

	/**
	 *
	 * @param queryStep
	 * @return a simplistic version of the queryStep, for logging purposes
	 */
	protected String simplistic(AdhocQueryStep queryStep) {
		return queryStep.getMeasure().getName();
	}

	/**
	 *
	 * @param queryStep
	 * @return a dense version of the queryStep, for logging purposes
	 */
	protected String dense(AdhocQueryStep queryStep) {
		// Do not log about debug, explainm or cache
		return new StringBuilder().append("m=")
				.append(queryStep.getMeasure().getName())
				.append(" filter=")
				.append(queryStep.getFilter())
				.append(" groupBy=")
				.append(queryStep.getGroupBy())
				.append(" custom=")
				.append(queryStep.getCustomMarker())
				.toString();
	}

	protected IMultitypeMergeableGrid<SliceAsMap> sinkToAggregates(ExecutingQueryContext executingQueryContext,
			TableQuery tableQuery,
			ITabularRecordStream stream,
			SetMultimap<String, Aggregator> columnToAggregators) {

		IAggregatedRecordStreamReducer streamReducer =
				makeAggregatedRecordStreamReducer(executingQueryContext, tableQuery, columnToAggregators);

		return streamReducer.reduce(stream);
	}

	protected IAggregatedRecordStreamReducer makeAggregatedRecordStreamReducer(
			ExecutingQueryContext executingQueryContext,
			TableQuery tableQuery,
			SetMultimap<String, Aggregator> columnToAggregators) {
		return AggregatedRecordStreamReducer.builder()
				.operatorsFactory(operatorsFactory)
				.executingQueryContext(executingQueryContext)
				.tableQuery(tableQuery)
				.columnToAggregators(columnToAggregators)
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

	protected void walkDagUpToQueriedMeasures(ExecutingQueryContext executingQueryContext,
			QueryStepsDag queryStepsDag,
			Map<AdhocQueryStep, ISliceToValue> queryStepToValues) {
		queryStepsDag.fromAggregatesToQueried().forEachRemaining(queryStep -> {

			if (queryStepToValues.containsKey(queryStep)) {
				// This typically happens on aggregator measures, as they are fed in a previous
				// step. Here, we want to process a measure once its underlying steps are completed
				return;
			} else if (queryStep.getMeasure() instanceof Aggregator a) {
				throw new IllegalStateException("Missing values for %s".formatted(a));
			}

			eventBus.post(QueryStepIsEvaluating.builder().queryStep(queryStep).source(this).build());

			IMeasure measure = executingQueryContext.resolveIfRef(queryStep.getMeasure());

			List<AdhocQueryStep> underlyingSteps = queryStepsDag.underlyingSteps(queryStep);

			IStopwatch stepWatch = stopwatchFactory.createStarted();

			Optional<ISliceToValue> optOutputColumn =
					processDagStep(queryStepToValues, queryStep, underlyingSteps, measure);

			// Would be empty on table steps (leaves of the DAG)
			optOutputColumn.ifPresent(outputColumn -> {
				Duration elapsed = stepWatch.elapsed();
				eventBus.post(QueryStepIsCompleted.builder()
						.querystep(queryStep)
						.nbCells(outputColumn.size())
						.source(this)
						.duration(elapsed)
						.build());
				queryStepsDag.registerExecutionFeedback(queryStep,
						SizeAndDuration.builder().size(outputColumn.size()).duration(elapsed).build());

				queryStepToValues.put(queryStep, outputColumn);
			});
		});
	}

	protected Optional<ISliceToValue> processDagStep(Map<AdhocQueryStep, ISliceToValue> queryStepToValues,
			AdhocQueryStep queryStep,
			List<AdhocQueryStep> underlyingSteps,
			IMeasure measure) {
		if (underlyingSteps.isEmpty()) {
			// This may happen on a Columnator which is missing a required column
			return Optional.empty();
		} else if (measure instanceof IHasUnderlyingMeasures hasUnderlyingMeasures) {
			List<ISliceToValue> underlyings = underlyingSteps.stream().map(underlyingStep -> {
				ISliceToValue values = queryStepToValues.get(underlyingStep);

				if (values == null) {
					throw new IllegalStateException("The DAG missed values for step=%s".formatted(underlyingStep));
				}

				return values;
			}).toList();

			// BEWARE It looks weird we have to call again `.wrapNode`
			ITransformator hasUnderlyingQuerySteps = hasUnderlyingMeasures.wrapNode(operatorsFactory, queryStep);
			ISliceToValue coordinatesToValues;
			try {
				coordinatesToValues = hasUnderlyingQuerySteps.produceOutputColumn(underlyings);
			} catch (RuntimeException e) {
				StringBuilder describeStep = new StringBuilder();

				describeStep.append("Issue computing columns for:").append("\r\n");

				// First, we print only measure as a simplistic shorthand of the step
				describeStep.append("    (measures) m=%s given %s".formatted(simplistic(queryStep),
						underlyingSteps.stream().map(this::simplistic).toList())).append("\r\n");
				// Second, we print the underlying steps as something may be hidden in filters, groupBys, configuration
				describeStep.append("    (steps) step=%s given %s".formatted(dense(queryStep),
						underlyingSteps.stream().map(this::dense).toList())).append("\r\n");

				throw new IllegalStateException(describeStep.toString(), e);
			}

			return Optional.of(coordinatesToValues);
		} else {
			throw new UnsupportedOperationException("%s".formatted(PepperLogHelper.getObjectAndClass(measure)));
		}
	}

	/**
	 * 
	 * @param executingQueryContext
	 * @param queryStepsDag
	 * @param queryStepToValues
	 * @return the output {@link ITabularView}
	 */
	protected ITabularView toTabularView(ExecutingQueryContext executingQueryContext,
			QueryStepsDag queryStepsDag,
			Map<AdhocQueryStep, ISliceToValue> queryStepToValues) {
		if (queryStepToValues.isEmpty()) {
			return MapBasedTabularView.empty();
		}

		long expectedOutputCardinality;
		Iterator<AdhocQueryStep> stepsToReturn;
		if (executingQueryContext.getOptions().contains(StandardQueryOptions.RETURN_UNDERLYING_MEASURES)) {
			// BEWARE Should we return steps with same groupBy?
			// BEWARE This does not work if there is multiple steps on same measure, as we later groupBy measureName
			// What about measures appearing multiple times in the DAG?
			stepsToReturn = new BreadthFirstIterator<>(queryStepsDag.getDag());
			expectedOutputCardinality = 0;
		} else {
			// BEWARE some queriedStep may be in the middle of the DAG if it is also the underlying of another step
			stepsToReturn = queryStepsDag.getQueried().iterator();
			expectedOutputCardinality =
					queryStepToValues.values().stream().mapToLong(ISliceToValue::size).max().getAsLong();
		}

		MapBasedTabularView mapBasedTabularView = MapBasedTabularView.builder()
				// Force a HashMap not to rely on default TreeMap
				.coordinatesToValues(new HashMap<>(Ints.saturatedCast(expectedOutputCardinality)))
				.build();

		stepsToReturn.forEachRemaining(step -> {
			ISliceToValue coordinatesToValues = queryStepToValues.get(step);
			if (coordinatesToValues == null) {
				// Happens on a Columnator missing a required column
			} else {
				boolean isEmptyMeasure = step.getMeasure() instanceof Aggregator agg
						&& EmptyAggregation.isEmpty(agg.getAggregationKey());

				boolean hasCarrierMeasure = executingQueryContext.getOptions()
						.contains(StandardQueryOptions.AGGREGATION_CARRIERS_STAY_WRAPPED)
						&& step.getMeasure() instanceof Aggregator agg
						&& operatorsFactory.makeAggregation(agg) instanceof IAggregationCarrier.IHasCarriers;

				IColumnScanner<SliceAsMap> baseRowScanner =
						slice -> mapBasedTabularView.sliceFeeder(slice, step.getMeasure().getName(), isEmptyMeasure);

				IColumnScanner<SliceAsMap> rowScanner;
				if (isEmptyMeasure) {
					rowScanner = slice -> {
						IValueReceiver sliceFeeder = baseRowScanner.onKey(slice);

						// `emptyValue` may not empty as we needed to materialize it to get up to here
						// But now is time to force it to null, while materializing the slice.
						return new IValueReceiver() {

							// This is useful to prevent boxing emptyValue when long
							@Override
							public void onLong(long v) {
								sliceFeeder.onObject(null);
							}

							// This is useful to prevent boxing emptyValue when double
							@Override
							public void onDouble(double v) {
								sliceFeeder.onObject(null);
							}

							@Override
							public void onObject(Object v) {
								sliceFeeder.onObject(null);
							}
						};
					};
				} else if (hasCarrierMeasure) {
					rowScanner = slice -> {
						IValueReceiver sliceFeeder = baseRowScanner.onKey(slice);

						// `emptyValue` may not empty as we needed to materialize it to get up to here
						// But now is time to force it to null, while materializing the slice.
						return new IValueReceiver() {

							// This is useful to prevent boxing emptyValue when long
							@Override
							public void onLong(long v) {
								sliceFeeder.onLong(v);
							}

							// This is useful to prevent boxing emptyValue when double
							@Override
							public void onDouble(double v) {
								sliceFeeder.onDouble(v);
							}

							@Override
							public void onObject(Object v) {
								if (v instanceof IAggregationCarrier aggregationCarrier) {
									// Transfer the carried value
									aggregationCarrier.acceptValueReceiver(sliceFeeder);
								} else {
									sliceFeeder.onObject(v);
								}
							}
						};
					};
				} else {
					rowScanner = baseRowScanner;
				}
				coordinatesToValues.forEachSlice(rowScanner);
			}
		});
		return mapBasedTabularView;
	}

}
