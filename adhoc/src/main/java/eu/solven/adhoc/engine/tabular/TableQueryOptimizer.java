package eu.solven.adhoc.engine.tabular;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.InternalQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.MoreFilterHelpers;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.table.TableQuery;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Standard {@link ITableQueryOptimizer}. It works on a per-Aggregator basis, evaluating which one can be skipped given
 * a minimal number of {@link CubeQueryStep}.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class TableQueryOptimizer implements ITableQueryOptimizer {

	/**
	 * 
	 * @param tableQueries
	 * @return an Object partitioning TableQuery which can not be induced from those which can be induced.
	 */
	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	@Override
	public SplitTableQueries splitInduced(IHasQueryOptions hasOptions, Set<TableQuery> tableQueries) {
		if (tableQueries.isEmpty()) {
			return SplitTableQueries.empty();
		}

		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> tableQueriesDag =
				new DirectedAcyclicGraph<>(DefaultEdge.class);

		// Inference in aggregator based: `k1` does not imply `k2`, `k1.SUM` does not imply `k1.MAX`
		SetMultimap<Aggregator, CubeQueryStep> aggregatorToQueries =
				MultimapBuilder.hashKeys().linkedHashSetValues().build();

		// Register all tableQueries as a vertex
		tableQueries.forEach(tq -> {
			tq.getAggregators().stream().forEach(agg -> {
				CubeQueryStep step = CubeQueryStep.edit(tq).measure(agg).build();
				tableQueriesDag.addVertex(step);
				aggregatorToQueries.put(agg, step);
			});
		});

		if (hasOptions.getOptions().contains(InternalQueryOptions.DISABLE_AGGREGATOR_INDUCTION)) {
			return SplitTableQueries.builder()
					.inducers(aggregatorToQueries.values())
					.tableQueriesDag(tableQueriesDag)
					.build();
		}

		// BEWARE Following algorithm is quadratic: for each tableQuery, we evaluate all other tableQuery.
		aggregatorToQueries.asMap().forEach((a, steps) -> {
			// groupBy number of groupedBy columns, in order to filter the candidate tableQueries
			int maxGroupBy =
					steps.stream().mapToInt(tb -> tb.getGroupBy().getGroupedByColumns().size()).max().getAsInt();
			List<Set<CubeQueryStep>> nbGroupByToQueries = IntStream.rangeClosed(0, maxGroupBy)
					.<Set<CubeQueryStep>>mapToObj(i -> new LinkedHashSet<>())
					.toList();

			// GroupBy tableQueries by groupBy cardinality, as we're guaranteed that a tableQuery with more groupBy can
			// not be inferred by a tableQUery with less groupBys.
			steps.forEach(step -> {
				nbGroupByToQueries.get(step.getGroupBy().getGroupedByColumns().size()).add(step);
			});

			steps.forEach(induced -> {
				AtomicBoolean hasFoundInducer = new AtomicBoolean();

				// right must have more groupBys than left, else right can not induce left
				for (int i = induced.getGroupBy().getGroupedByColumns().size(); i < nbGroupByToQueries.size(); i++) {
					nbGroupByToQueries.get(i)
							.stream()
							// No edge to itself
							.filter(inducer -> inducer != induced)
							// Same context (i.e. same filter, customMarker, options)
							.filter(inducer -> canInduce(inducer, induced))
							// as soon as left is induced, we do not need to search for alternative inducer
							// BEWARE As we find first, we'll spot the inducer with a minimal additional groupBy, hence
							// probably a smaller induced, hence probably a better inducer
							.findFirst()
							.ifPresent(inducer -> {
								// right can be used to compute left
								tableQueriesDag.addEdge(inducer, induced);
								hasFoundInducer.set(true);

								if (hasOptions.isDebugOrExplain()) {
									log.info("[EXPLAIN] {} will induce {}", inducer, induced);
								}
							});

					if (hasFoundInducer.get()) {
						break;
					}
				}
			});
		});

		// Collect the tableQueries which can not be induced by another tableQuery
		Set<CubeQueryStep> notInduced = tableQueriesDag.vertexSet()
				.stream()
				.filter(tq -> tableQueriesDag.incomingEdgesOf(tq).isEmpty())
				.collect(ImmutableSet.toImmutableSet());
		// Collect the tableQueries which can be induced
		Set<CubeQueryStep> induced = ImmutableSet.copyOf(Sets.difference(tableQueriesDag.vertexSet(), notInduced));

		return SplitTableQueries.builder()
				.inducers(notInduced)
				.induceds(induced)
				.tableQueriesDag(tableQueriesDag)
				.build();
	}

	// Typically: `groupBy:ccy&ccy=EUR|USD` can induce `ccy=EUR`
	protected boolean canInduce(CubeQueryStep inducer, CubeQueryStep induced) {
		if (!inducer.getMeasure().getName().equals(induced.getMeasure().getName())) {
			// Different measures: can not induce
			return false;
		} else if (!asMeasureless(inducer).equals(asMeasureless(induced))) {
			// Different options/customMarker: can not induce
			return false;
		}

		NavigableSet<String> inducerColumns = inducer.getGroupBy().getGroupedByColumns();
		NavigableSet<String> inducedColumns = induced.getGroupBy().getGroupedByColumns();
		if (!inducerColumns.containsAll(inducedColumns)) {
			// Not expressing all needed columns: can not induce
			// If right has all groupBy of left, it means right has same or more groupBy than left,
			// hence right can be used to compute left
			return false;
		}

		IAdhocFilter inducerFilter = inducer.getFilter();
		IAdhocFilter inducedFilter = induced.getFilter();

		if (!AndFilter.and(inducerFilter, inducedFilter).equals(inducedFilter)) {
			// Inducer is stricter than induced: it can not infer it
			return false;
		}

		if (inducerColumns.containsAll(FilterHelpers.getFilteredColumns(inducedFilter))) {
			// Inducer has enough columns to apply induced filter
			return true;
		}

		// We may lack column
		if (inducerFilter.equals(inducedFilter)) {
			return true;
		}

		// TODO This comparison should be done only on the filter in induced not present in inducer
		return false;
	}

	/**
	 * Check everything representing the context of the query. Typically represents the {@link IQueryOption} and the
	 * customMarker.
	 * 
	 * @param inducer
	 * @return a CubeQueryStep which has been fleshed-out of what's not the query context.
	 */
	protected CubeQueryStep asMeasureless(CubeQueryStep inducer) {
		return CubeQueryStep.edit(inducer)
				.measure("noMeasure")
				.groupBy(IAdhocGroupBy.GRAND_TOTAL)
				.filter(IAdhocFilter.MATCH_ALL)
				.build();
	}

	@Override
	public IMultitypeMergeableColumn<SliceAsMap> evaluateInduced(AdhocFactories factories,
			IHasQueryOptions hasOptions,
			SplitTableQueries inducerAndInduced,
			Map<CubeQueryStep, ISliceToValue> stepToValues,
			CubeQueryStep induced) {
		@NonNull
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag = inducerAndInduced.getTableQueriesDag();

		List<CubeQueryStep> inducers = dag.incomingEdgesOf(induced).stream().map(dag::getEdgeSource).toList();
		if (inducers.size() != 1) {
			throw new IllegalStateException(
					"Induced should have a single inducer. induced=%s inducers=%s".formatted(induced, inducers));
		}

		CubeQueryStep inducer = inducers.getFirst();
		ISliceToValue inducerValues = stepToValues.get(inducer);

		Aggregator aggregator = (Aggregator) inducer.getMeasure();
		IAggregation aggregation = factories.getOperatorFactory().makeAggregation(aggregator);
		IMultitypeMergeableColumn<SliceAsMap> inducedValues =
				factories.getColumnsFactory().makeColumn(aggregation, List.of(inducerValues));

		inducerValues.forEachSlice(
				slice -> inducedValues.merge(inducedGroupBy(induced.getGroupBy().getGroupedByColumns(), slice)));

		if (hasOptions.isDebugOrExplain()) {
			Set<String> removedGroupBys = Sets.difference(inducer.getGroupBy().getGroupedByColumns(),
					induced.getGroupBy().getGroupedByColumns());
			log.info("[EXPLAIN] size={} induced size={} by removing groupBy={} ({} induced {})",
					inducerValues.size(),
					inducedValues.size(),
					removedGroupBys,
					inducer,
					induced);
		}

		return inducedValues;
	}

	protected SliceAsMap inducedGroupBy(NavigableSet<String> groupedByColumns, SliceAsMap inducer) {
		Map<String, Object> induced = new LinkedHashMap<>();

		groupedByColumns.forEach(inducedColumn -> {
			induced.put(inducedColumn, inducer.getRawSliced(inducedColumn));
		});

		// TODO Rely on AdhocMap
		return SliceAsMap.fromMap(induced);
	}

}
