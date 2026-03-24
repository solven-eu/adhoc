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
package eu.solven.adhoc.engine.tabular.optimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.Graphs;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.dataframe.tabular.primitives.Int2ObjectBiConsumer;
import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ICubeQueryStep;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.grouper.ITableStepsGrouper;
import eu.solven.adhoc.engine.tabular.grouper.TableStepsGrouper;
import eu.solven.adhoc.engine.tabular.grouper.TableStepsGrouperByAffinity;
import eu.solven.adhoc.engine.tabular.grouper.TableStepsGrouperByAggregator;
import eu.solven.adhoc.engine.tabular.grouper.TableStepsGrouperNoGroup;
import eu.solven.adhoc.engine.tabular.optimizer.IHasTableQueryForSteps.StepAndFilteredAggregator;
import eu.solven.adhoc.engine.tabular.splitter.ITableStepsSplitter;
import eu.solven.adhoc.engine.tabular.splitter.InduceByAdhocComplete;
import eu.solven.adhoc.engine.tabular.splitter.InduceByTableWrapper;
import eu.solven.adhoc.filter.FilterEquivalencyHelpers;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.OrFilter;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.table.ITableWrapper;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * The main strategy of this {@link ITableQueryFactory} is to evaluate the minimal number of {@link TableQuery} needed
 * to compute all {@link CubeQueryStep} allowing to compute irrelevant aggregates. Typically, it will evaluate the union
 * of {@link IGroupBy} and an {@link OrFilter} amongst all {@link ISliceFilter}.
 * 
 * In short, it enables doing a single query per measure to the {@link ITableWrapper}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class TableQueryFactory extends ATableQueryFactory {

	protected final ITableStepsSplitter splitter;
	protected final ITableStepsGrouper grouper;

	// Rely on a filterOptimizer with cache as this tableQueryOptimizer may collect a large number of filters into
	// a single query, leading to a very large OR.
	@Builder
	public TableQueryFactory(IAdhocFactories factories,
			IFilterOptimizer filterOptimizer,
			ITableStepsSplitter splitter,
			ITableStepsGrouper grouper) {
		super(factories, filterOptimizer);

		this.splitter = splitter;
		this.grouper = grouper;
	}

	// Rely on a filterOptimizer with cache as this tableQueryOptimizer may collect a large number of filters into
	// a single query, leading to a very large OR.
	public TableQueryFactory(IAdhocFactories factories, IFilterOptimizer filterOptimizer) {
		this(factories, filterOptimizer, new InduceByAdhocComplete(), new TableStepsGrouper());
	}

	@Override
	public SplitTableQueries splitInduced(IHasQueryOptions hasOptions, Set<TableQueryStep> tableSteps) {
		if (tableSteps.isEmpty()) {
			return SplitTableQueries.empty();
		}

		IAdhocDag<TableQueryStep> inducedToInducer = GraphHelpers.makeGraph();
		// Initialize the DAG of flat tableSteps
		tableSteps.forEach(inducedToInducer::addVertex);

		// Add the additional vertices and edges
		Graphs.addGraph(inducedToInducer, splitter.splitInducedAsDag(hasOptions, inducedToInducer));

		Map<TableQueryStep, TableQueryV4> stepToTableQuery = makeStepToTableQuery(tableSteps, inducedToInducer);

		SplitTableQueries splitTableQueries = SplitTableQueries.builder()
				.explicits(tableSteps)
				.inducedToInducer(inducedToInducer)
				.stepToTables(stepToTableQuery)
				.build();

		// Sanity checks will typically ensure the tableQueries covers all leaves inducers
		sanityChecks(splitTableQueries);

		if (hasOptions.isDebugOrExplain()) {
			onDebugOrExplain(tableSteps, splitTableQueries);
		}

		return splitTableQueries;
	}

	protected void onDebugOrExplain(Set<TableQueryStep> tableSteps, SplitTableQueries splitTableQueries) {
		Set<TableQueryV4> tableQueries = splitTableQueries.getTableQueries();

		int nbTableInducers = splitTableQueries.getInducers().size();
		// This represents the number of CubeQueryStep evaluated by the tableQuery, amongst which a bunch are
		// possibly useless.
		// BEWARE If customMarker are suppressed from tableQueries, these numbers would need additional
		// interpretations
		long nbEvaluatedTableInducers =
				tableQueries.stream().map(TableQueryV4::asCoveringV3).mapToLong(TableQueryV3::nbCuboids).sum();

		// prints percent with 1 digit.
		String percentEfficiency = asPercent(tableSteps.size(), nbEvaluatedTableInducers);
		log.info(
				"[EXPLAIN] {} steps led to {} inducers evaluated by {} tableQueries (evaluating {} steps). Efficiency={}",
				tableSteps.size(),
				nbTableInducers,
				tableQueries.size(),
				nbEvaluatedTableInducers,
				percentEfficiency);

		forEachIndexed(tableQueries, (indexQuery, tableQuery) -> {
			log.info("[EXPLAIN] TableQuery {}/{}: {}", indexQuery, tableQueries.size(), tableQuery);
		});
	}

	@SuppressWarnings("checkstyle:MagicNumber")
	public static String asPercent(long nbNeededSteps, long nbEvaluatedSteps) {
		return "%.1f%%".formatted(100.0 * nbNeededSteps / nbEvaluatedSteps);
	}

	protected Map<TableQueryStep, TableQueryV4> makeStepToTableQuery(Set<TableQueryStep> tableSteps,
			IAdhocDag<TableQueryStep> inducedToInducer) {
		Set<TableQueryStep> inducers = GraphHelpers.getInducers(inducedToInducer);

		Collection<? extends Collection<TableQueryStep>> groups = grouper.groupInducers(inducers);

		Map<TableQueryStep, TableQueryV4> stepToTableQuery = new LinkedHashMap<>();

		groups.forEach(group -> {
			TableQueryStep context = grouper.tableQueryGroupBy(group.iterator().next());
			TableQueryV4 v4 = processRelatedSteps(context, new ArrayList<>(group));
			group.forEach(step -> stepToTableQuery.put(step, v4));
		});

		return stepToTableQuery;
	}

	protected TableQueryV4 processRelatedSteps(ICubeQueryStep context, List<TableQueryStep> steps) {
		// BEWARE Calculated columns (where a groupBy column is not a ReferencedColumn) previously required separate
		// queries to avoid GROUPING SET issues. With TableQueryV4, streamV3() naturally gives each groupBy with a
		// unique aggregator set its own V3 sub-query, which resolves the conflict in most cases.
		// TODO Verify that calculated columns with the same aggregator set as a non-calculated groupBy are handled
		// correctly by streamV3().
		return makeTableQueryV4(context, steps);
	}

	/**
	 * Checks the tableQueries are actually valid: do they cover the required steps?
	 * 
	 * @param inducerAndInduced
	 */
	protected void sanityChecks(SplitTableQueries inducerAndInduced) {
		Set<TableQueryV4> tableQueries = inducerAndInduced.getTableQueries();

		// Checks the steps from tableQueries covers the inducers
		sanityCheckInducers(inducerAndInduced, tableQueries);

		// Check inducing covers all expected steps
		sanityCheckExplicits(inducerAndInduced);
	}

	public static <T> void forEachIndexed(Collection<T> c, Int2ObjectBiConsumer<T> consumer) {
		int index = 0;
		for (T one : c) {
			index++;
			consumer.acceptInt2Object(index, one);
		}
	}

	protected void sanityCheckExplicits(SplitTableQueries inducerAndInduced) {
		Set<TableQueryStep> stepsInducedFromTable = inducerAndInduced.getInducedToInducer().vertexSet();

		// Given all tableDag nodes, we should have all cubeDag roots
		ImmutableSet<TableQueryStep> requestedStepFromDag = inducerAndInduced.getExplicits();
		Set<TableQueryStep> missingFromTable = Sets.difference(requestedStepFromDag, stepsInducedFromTable);
		if (!missingFromTable.isEmpty()) {
			int nbMissing = missingFromTable.size();
			{
				log.warn("Missing {} steps from tableQueries to fill cube DAG roots", nbMissing);
				forEachIndexed(missingFromTable, (indexMissing, missingStep) -> {
					log.warn("Missing {}/{}: {}", indexMissing, nbMissing, missingStep);
				});
			}

			{
				log.warn("requested steps (from cube DAG):");
				forEachIndexed(requestedStepFromDag, (indexRequested, availableStep) -> {
					log.warn("Requested {}/{}: {}", indexRequested, requestedStepFromDag.size(), availableStep);
				});
			}

			{
				log.warn("provided steps (from tableQueries):");
				forEachIndexed(stepsInducedFromTable, (indexExpected, availableStep) -> {
					log.warn("Provided {}/{}: {}", indexExpected, stepsInducedFromTable.size(), availableStep);
				});
			}

			// Take the shorter/simpler problematic entry
			TableQueryStep firstMissing =
					missingFromTable.stream().min(Comparator.comparing(s -> s.toString().length())).get();
			log.warn("Analyzing one missing: {}", firstMissing);
			Set<TableQueryStep> impliedSameMeasure = stepsInducedFromTable.stream()
					.filter(s -> s.getMeasure().getName().equals(firstMissing.getMeasure().getName()))
					.collect(ImmutableSet.toImmutableSet());
			log.warn("Missing has {} sameMeasure siblings", impliedSameMeasure.size());

			Set<TableQueryStep> impliedSameMeasureSameGroupBy = impliedSameMeasure.stream()
					.filter(s -> s.getGroupBy()
							.getGroupedByColumns()
							.equals(firstMissing.getGroupBy().getGroupedByColumns()))
					.collect(ImmutableSet.toImmutableSet());
			log.warn("Missing has {} sameMeasureAndGroupBy siblings", impliedSameMeasureSameGroupBy.size());

			Set<TableQueryStep> impliedSameMeasureSameGroupBySameFilter = impliedSameMeasureSameGroupBy.stream()
					.filter(s -> s.getFilter().equals(firstMissing.getFilter()))
					.collect(ImmutableSet.toImmutableSet());
			log.warn("Missing has {} sameMeasureSameGroupBySameFilter siblings",
					impliedSameMeasureSameGroupBySameFilter.size());

			Set<TableQueryStep> impliedSameMeasureSameGroupByEquivalentFilter = impliedSameMeasureSameGroupBy.stream()
					.filter(s -> FilterEquivalencyHelpers.areEquivalent(s.getFilter(), firstMissing.getFilter()))
					.collect(ImmutableSet.toImmutableSet());
			log.warn("Missing has {} sameMeasureSameGroupByEquivalentFilter siblings",
					impliedSameMeasureSameGroupByEquivalentFilter.size());

			// This typically happens due to inconsistency in equality if ISliceFiler (e.g. `a` and
			// `Not(Not(a))`)
			throw new IllegalStateException(
					"Missing %s steps from tableQueries+induceProcess to fill cube DAG tableSteps"
							.formatted(nbMissing));
		}
	}

	protected void sanityCheckInducers(SplitTableQueries inducerAndInduced, Set<TableQueryV4> tableQueries) {
		// Holds the querySteps evaluated from the ITableWrapper
		Set<TableQueryStep> receivedInducerSteps = tableQueries.stream()
				.flatMap(tq -> inducerAndInduced.forEachCubeQuerySteps(tq, filterOptimizer))
				.map(StepAndFilteredAggregator::step)
				.collect(ImmutableSet.toImmutableSet());

		// tableDag will evaluate from table querySteps to cubeDag root querySteps
		Set<TableQueryStep> expectedInducerSteps = inducerAndInduced.getInducers();

		Set<TableQueryStep> missingRootsFromTableQueries = Sets.difference(expectedInducerSteps, receivedInducerSteps);
		if (!missingRootsFromTableQueries.isEmpty()) {
			int nbMissing = missingRootsFromTableQueries.size();

			{
				log.warn("Missing {} steps from tableQueries to fill table DAG roots", nbMissing);
				int indexMissing = 0;
				for (TableQueryStep missingStep : missingRootsFromTableQueries) {
					indexMissing++;
					log.warn("Missing {}/{}: {}", indexMissing, nbMissing, missingStep);

					receivedInducerSteps.stream()
							// This issue is probably due to a faulty filter representation: we search for steps
							// differing only by filter
							.filter(s -> suppressFilter(s).equals(suppressFilter(missingStep)))
							.forEach(queryDifferingByFilter -> {
								log.warn("\\-- Relates with {}", queryDifferingByFilter);
							});
				}
			}

			{
				log.warn("requested steps (from cube DAG):");
				int indexRequested = 0;
				int nbRequested = expectedInducerSteps.size();
				for (TableQueryStep availableStep : expectedInducerSteps) {
					indexRequested++;
					log.warn("Requested {}/{}: {}", indexRequested, nbRequested, availableStep);
				}
			}

			{
				log.warn("provided steps (from tableQueries):");
				int indexExpected = 0;
				int nbProvided = receivedInducerSteps.size();
				for (TableQueryStep availableStep : receivedInducerSteps) {
					indexExpected++;
					log.warn("Provided {}/{}: {}", indexExpected, nbProvided, availableStep);
				}
			}

			// This typically happens due to inconsistency in equality if ISliceFiler (e.g. `a` and
			// `Not(Not(a))`)
			throw new IllegalStateException(
					"Missing %s steps from tableQueries to cover inducers".formatted(nbMissing));
		}
	}

	protected TableQueryStep suppressFilter(TableQueryStep s) {
		return TableQueryStep.edit(s).filter(ISliceFilter.MATCH_ALL).build();
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add("grouper", grouper).add("splitter", splitter).toString();
	}

	/**
	 * Lombok @Builder
	 */
	public static class TableQueryFactoryBuilder {
		public TableQueryFactoryBuilder splitForAdhocInference() {
			return this.splitter(new InduceByAdhocComplete());
		}

		public TableQueryFactoryBuilder splitForTableGroupingSets() {
			return this.splitter(new InduceByTableWrapper());
		}

		public TableQueryFactoryBuilder groupByAggregator() {
			return this.grouper(new TableStepsGrouperByAggregator());
		}

		public TableQueryFactoryBuilder singleTableQuery() {
			return this.grouper(new TableStepsGrouper());
		}

		public TableQueryFactoryBuilder oneTableQueryperStep() {
			return this.grouper(new TableStepsGrouperNoGroup());
		}

		public TableQueryFactoryBuilder groupByAffinity() {
			return this.grouper(new TableStepsGrouperByAffinity());
		}

	}
}
