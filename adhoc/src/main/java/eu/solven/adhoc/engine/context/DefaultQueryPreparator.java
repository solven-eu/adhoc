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
package eu.solven.adhoc.engine.context;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;

import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.engine.ICanResolveMeasure;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.MeasureForest.MeasureForestBuilder;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.AdhocSubQuery;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * Default implementation of {@link IQueryPreparator}.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
@RequiredArgsConstructor
public class DefaultQueryPreparator implements IQueryPreparator {

	// By default, the filters are not modified
	@NonNull
	@Default
	final IImplicitFilter implicitFilter = query -> IAdhocFilter.MATCH_ALL;

	@NonNull
	@Default
	final IImplicitOptions implicitOptions = query -> ImmutableSet.of();

	@NonNull
	@Default
	final ExecutorService concurrentExecutorService = AdhocUnsafe.adhocCommonPool;

	// If not-concurrent, we want the simpler execution plan as possible
	// it is especially important to simplify thread-jumping lowering readability of stack-traces
	@NonNull
	@Default
	final ExecutorService nonConcurrentExecutorService = MoreExecutors.newDirectExecutorService();

	@Override
	public QueryPod prepareQuery(ITableWrapper table,
			IMeasureForest forest,
			IColumnsManager columnsManager,
			ICubeQuery rawQuery) {
		ICubeQuery preparedQuery = combineWithImplicit(rawQuery);
		AdhocQueryId queryId = AdhocQueryId.from(table.getName(), preparedQuery);

		QueryPod fullQueryPod = QueryPod.builder()
				.query(preparedQuery)
				.queryId(queryId)
				.forest(forest)
				.table(table)
				.columnsManager(columnsManager)
				.executorService(getExecutorService(preparedQuery))
				.build();

		// Filtering the forst is useful for edge-cades like:
		// - columnGenerator: we should consider only measures in the queryPlan
		IMeasureForest relevantForest =
				filterForest(fullQueryPod, preparedQuery).name(forest.getName() + "-filtered").build();

		return fullQueryPod.toBuilder().forest(relevantForest).build();
	}

	/**
	 * 
	 * @param forest
	 * @param preparedQuery
	 * @return a {@link MeasureForest} restricted to the measures playing a role in {@link ICubeQuery}.
	 */
	@SuppressWarnings("PMD.CollapsibleIfStatements")
	protected MeasureForestBuilder filterForest(ICanResolveMeasure forest, ICubeQuery preparedQuery) {
		Set<IMeasure> relevantMeasures = new LinkedHashSet<>();

		Set<IMeasure> measuresToAdd = new LinkedHashSet<>(preparedQuery.getMeasures());
		while (!measuresToAdd.isEmpty()) {
			Set<String> nextMeasuresToAdd = new LinkedHashSet<>();

			measuresToAdd.forEach(measureToAdd -> {
				IMeasure resolvedMeasure = forest.resolveIfRef(measureToAdd);

				if (relevantMeasures.add(resolvedMeasure)) {

					// BEWARE Not `IHasUnderlyingNames` as it is informative. e.g. in Composite Cube, measure may list
					// underlying measures in underlying cubes, while in compositeCube.queryPlan, these are irrelevant.
					if (resolvedMeasure instanceof IHasUnderlyingMeasures hasUnderlyings) {
						nextMeasuresToAdd.addAll(hasUnderlyings.getUnderlyingNames());
					}
				}
			});

			measuresToAdd.clear();
			nextMeasuresToAdd.forEach(m -> measuresToAdd.add(forest.resolveIfRef(ReferencedMeasure.ref(m))));
		}

		return MeasureForest.builder().measures(relevantMeasures);
	}

	protected ExecutorService getExecutorService(ICubeQuery preparedQuery) {
		if (preparedQuery.getOptions().contains(StandardQueryOptions.CONCURRENT)) {
			// Concurrent query: execute in a dedicated pool
			return concurrentExecutorService;
		} else {
			// Not concurrent query: rely on current thread
			return nonConcurrentExecutorService;
		}
	}

	protected ICubeQuery combineWithImplicit(ICubeQuery rawQuery) {
		IAdhocFilter preprocessedFilter =
				AndFilter.and(rawQuery.getFilter(), implicitFilter.getImplicitFilter(rawQuery));

		Set<IQueryOption> addedOptions = implicitOptions.getOptions(rawQuery);
		CubeQuery query = CubeQuery.edit(rawQuery).filter(preprocessedFilter).options(addedOptions).build();

		if (rawQuery instanceof AdhocSubQuery subQuery) {
			return subQuery.toBuilder().subQuery(query).build();
		} else {
			return query;
		}
	}

}
