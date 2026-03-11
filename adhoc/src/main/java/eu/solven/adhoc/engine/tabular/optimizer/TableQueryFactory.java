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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.splitter.ITableStepsGrouper;
import eu.solven.adhoc.engine.tabular.splitter.ITableStepsSplitter;
import eu.solven.adhoc.engine.tabular.splitter.InduceByAdhoc;
import eu.solven.adhoc.engine.tabular.splitter.InduceByGroupingSets;
import eu.solven.adhoc.engine.tabular.splitter.TableStepsGrouper;
import eu.solven.adhoc.engine.tabular.splitter.TableStepsGrouperByAggregator;
import eu.solven.adhoc.engine.tabular.splitter.TableStepsGrouperNoGroup;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.query.cube.IGroupBy;
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

		return SplitTableQueries.builder()
				.explicits(tableSteps)
				.inducedToInducer(inducedToInducer)
				.stepToTables(stepToTableQuery)
				.build();
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
