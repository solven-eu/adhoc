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

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.IHasTableQueryForSteps.StepAndFilteredAggregator;
import eu.solven.adhoc.engine.tabular.splitter.ITableStepsGrouper;
import eu.solven.adhoc.engine.tabular.splitter.ITableStepsSplitter;
import eu.solven.adhoc.engine.tabular.splitter.InduceByAdhoc;
import eu.solven.adhoc.engine.tabular.splitter.InduceByGroupingSets;
import eu.solven.adhoc.engine.tabular.splitter.TableStepsGrouper;
import eu.solven.adhoc.engine.tabular.splitter.TableStepsGrouperByAggregator;
import eu.solven.adhoc.engine.tabular.splitter.TableStepsGrouperNoGroup;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.filter.FilterEquivalencyHelpers;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV3;
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
		this(factories, filterOptimizer, new InduceByAdhoc(), new TableStepsGrouper());
	}

	@Override
	public SplitTableQueries splitInduced(IHasQueryOptions hasOptions, Set<CubeQueryStep> tableSteps) {
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducer =
				splitter.splitInducedAsDag(hasOptions, tableSteps);

		Map<CubeQueryStep, TableQueryV3> stepToTableQuery = makeStepToTableQuery(tableSteps, inducedToInducer);

		SplitTableQueries splitTableQueries = SplitTableQueries.builder()
				.explicits(tableSteps)
				.inducedToInducer(inducedToInducer)
				.stepToTables(stepToTableQuery)
				.build();

		sanityChecks(tableSteps, splitTableQueries);

		if (hasOptions.isDebugOrExplain()) {
			Set<TableQueryV3> tableQueries = splitTableQueries.getTableQueries();

			// This represents the number of CubeQueryStep evaluated by the tableQuery, amongst which a bunch are
			// possibly useless.
			// BEWARE If customMarker are suppressed from tableQueries, these numbers would need additional
			// interpretations
			long nbOutputSteps = tableQueries.stream()
					.mapToLong(tq -> tq.getAggregators().size() * tq.streamGroupBy().count())
					.sum();

			log.info("[EXPLAIN] {} steps led to {} inducers evaluated by {} tableQueries (evaluating {} steps)",
					tableSteps.size(),
					splitTableQueries.getInducers().size(),
					tableQueries.size(),
					nbOutputSteps);
		}

		return splitTableQueries;
	}

	protected Map<CubeQueryStep, TableQueryV3> makeStepToTableQuery(Set<CubeQueryStep> tableSteps,
			DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducer) {
		Set<CubeQueryStep> leaves = SplitTableQueries.builder()
				.explicits(tableSteps)
				.inducedToInducer(inducedToInducer)
				.build()
				.getInducers();

		Map<CubeQueryStep, List<CubeQueryStep>> contextToSteps =
				leaves.stream().collect(Collectors.groupingBy(grouper::tableQueryGroupBy));

		Map<CubeQueryStep, TableQueryV3> stepToTableQuery = new LinkedHashMap<>();

		contextToSteps.forEach((context, steps) -> stepToTableQuery.putAll(processRelatedSteps(context, steps)));

		return stepToTableQuery;
	}

	protected Map<CubeQueryStep, TableQueryV3> processRelatedSteps(CubeQueryStep context, List<CubeQueryStep> steps) {
		// BEWARE There is difficulties around GROUPING SET and calculated columns
		// (e.g. confusion between a reference to column `c`, and a calculated column based on `c` which will actually
		// do a grandTotal (e.g. the `*` coordinate)).
		Map<Boolean, List<CubeQueryStep>> partitioningByCalculated = steps.stream()
				.collect(Collectors.partitioningBy(s -> s.getGroupBy()
						.getNameToColumn()
						.values()
						.stream()
						.anyMatch(c -> !(c instanceof ReferencedColumn))));

		Map<CubeQueryStep, TableQueryV3> stepToTableQuery = new LinkedHashMap<>();

		// Add one tableQuery without any calculated columns
		stepToTableQuery.putAll(processSteps(context, partitioningByCalculated.get(false)));

		// Add one tableQuery per step with at least one calculated column
		// BEWARE Should work on this necessity as it looks not legitimate and very sub-optimal
		partitioningByCalculated.get(true).forEach(notOnlyRef -> {
			stepToTableQuery.putAll(processSteps(context, ImmutableSet.of(notOnlyRef)));
		});

		return stepToTableQuery;
	}

	protected Map<CubeQueryStep, TableQueryV3> processSteps(CubeQueryStep context, Collection<CubeQueryStep> steps) {
		if (steps.isEmpty()) {
			return Map.of();
		}

		TableQueryV3 query = makeTableQuery(context, steps);

		Map<CubeQueryStep, TableQueryV3> stepToTableQuery = new LinkedHashMap<>();
		steps.forEach(step -> stepToTableQuery.put(step, query));
		return stepToTableQuery;
	}

	/**
	 * Checks the tableQueries are actually valid: do they cover the required steps?
	 * 
	 * @param tableSteps
	 * @param inducerAndInduced
	 */
	protected void sanityChecks(Set<CubeQueryStep> tableSteps, SplitTableQueries inducerAndInduced) {
		Set<TableQueryV3> tableQueries = inducerAndInduced.getTableQueries();

		// Holds the querySteps evaluated from the ITableWrapper
		Set<CubeQueryStep> queryStepsFromTableQueries = tableQueries.stream()
				.flatMap(tq -> inducerAndInduced.forEachCubeQuerySteps(tq, filterOptimizer))
				.map(StepAndFilteredAggregator::step)
				.collect(ImmutableSet.toImmutableSet());

		// tableDag will evaluate from table querySteps to cubeDag root querySteps
		{
			Set<CubeQueryStep> tableRoots = inducerAndInduced.getInducers();

			Set<CubeQueryStep> missingRootsFromTableQueries = Sets.difference(tableRoots, queryStepsFromTableQueries);
			if (!missingRootsFromTableQueries.isEmpty()) {
				int nbMissing = missingRootsFromTableQueries.size();
				log.warn("Missing {} steps from tableQueries to fill table DAG roots", nbMissing);

				int indexMissing = 0;
				for (CubeQueryStep missingStep : missingRootsFromTableQueries) {
					indexMissing++;
					log.warn("Missing {}/{}: {}", indexMissing, nbMissing, missingStep);

					queryStepsFromTableQueries.stream()
							// This issue is probably due to a faulty filter representation: we search for steps
							// differing only by filter
							.filter(s -> suppressFilter(s).equals(suppressFilter(missingStep)))
							.forEach(queryDifferingByFilter -> {
								log.warn("\\-- Relates with {}", queryDifferingByFilter);
							});

				}

				// This typically happens due to inconsistency in equality if ISliceFiler (e.g. `a` and
				// `Not(Not(a))`)
				throw new IllegalStateException(
						"Missing %s steps from tableQueries to fill table DAG roots".formatted(nbMissing));
			}
		}

		Set<CubeQueryStep> stepsImpliedByTableQueries = inducerAndInduced.getInducedToInducer().vertexSet();

		// Given all tableDag nodes, we should have all cubeDag roots
		{
			Set<CubeQueryStep> missingFromTable = Sets.difference(tableSteps, stepsImpliedByTableQueries);
			if (!missingFromTable.isEmpty()) {
				int nbMissing = missingFromTable.size();
				log.warn("Missing {} steps from tableQueries to fill cube DAG roots", nbMissing);
				int indexMissing = 0;
				for (CubeQueryStep missingStep : missingFromTable) {
					indexMissing++;
					log.warn("Missing {}/{}: {}", indexMissing, nbMissing, missingStep);
				}

				// Take the shorter/simpler problematic entry
				CubeQueryStep firstMissing =
						missingFromTable.stream().min(Comparator.comparing(s -> s.toString().length())).get();
				log.warn("Analyzing one missing: {}", firstMissing);
				Set<CubeQueryStep> impliedSameMeasure = stepsImpliedByTableQueries.stream()
						.filter(s -> s.getMeasure().getName().equals(firstMissing.getMeasure().getName()))
						.collect(ImmutableSet.toImmutableSet());
				log.warn("Missing has {} sameMeasure siblings", impliedSameMeasure.size());

				Set<CubeQueryStep> impliedSameMeasureSameGroupBy = impliedSameMeasure.stream()
						.filter(s -> s.getGroupBy()
								.getGroupedByColumns()
								.equals(firstMissing.getGroupBy().getGroupedByColumns()))
						.collect(ImmutableSet.toImmutableSet());
				log.warn("Missing has {} sameMeasureAndGroupBy siblings", impliedSameMeasureSameGroupBy.size());

				Set<CubeQueryStep> impliedSameMeasureSameGroupBySameFilter = impliedSameMeasureSameGroupBy.stream()
						.filter(s -> s.getFilter().equals(firstMissing.getFilter()))
						.collect(ImmutableSet.toImmutableSet());
				log.warn("Missing has {} sameMeasureSameGroupBySameFilter siblings",
						impliedSameMeasureSameGroupBySameFilter.size());

				Set<CubeQueryStep> impliedSameMeasureSameGroupByEquivalentFilter = impliedSameMeasureSameGroupBy
						.stream()
						.filter(s -> FilterEquivalencyHelpers.areEquivalent(s.getFilter(), firstMissing.getFilter()))
						.collect(ImmutableSet.toImmutableSet());
				log.warn("Missing has {} sameMeasureSameGroupByEquivalentFilter siblings",
						impliedSameMeasureSameGroupByEquivalentFilter.size());

				// This typically happens due to inconsistency in equality if ISliceFiler (e.g. `a` and
				// `Not(Not(a))`)
				throw new IllegalStateException(
						"Missing %s steps from tableQueries to fill cube DAG roots".formatted(nbMissing));
			}
		}

		// Set<CubeQueryStep> irrelevantComputations = Sets.difference(queryStepsFromTableQueries, missingTableRoots);
		//
		// if (!irrelevantComputations.isEmpty()) {
		// // Typically happens with TableQueryOptimizerSinglePerAggregator
		// int nbIrrelevant = irrelevantComputations.size();
		// log.info("Irrelevant {} steps from tableQueries to fill DAG roots", nbIrrelevant);
		// int indexIrrelevant = 0;
		// for (CubeQueryStep irrelevantStep : irrelevantComputations) {
		// indexIrrelevant++;
		// log.warn("Irrelevant {}/{}: {}", indexIrrelevant, nbIrrelevant, irrelevantStep);
		// }
		// }
	}

	protected CubeQueryStep suppressFilter(CubeQueryStep s) {
		return CubeQueryStep.edit(s).filter(ISliceFilter.MATCH_ALL).build();
	}

	/**
	 * Lombok @Builder
	 */
	public static class TableQueryFactoryBuilder {
		public TableQueryFactoryBuilder splitForAdhocInference() {
			return this.splitter(new InduceByAdhoc());
		}

		public TableQueryFactoryBuilder splitForTableGroupingSets() {
			return this.splitter(new InduceByGroupingSets());
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

	}
}
