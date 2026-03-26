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

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.column.generated_column.IMayHaveColumnGenerator;
import eu.solven.adhoc.engine.cache.IQueryStepCache;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.IHasMeasures;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.forest.IMeasureForest;
import eu.solven.adhoc.measure.forest.IMeasureResolver;
import eu.solven.adhoc.measure.forest.MeasureForest;
import eu.solven.adhoc.measure.forest.MeasureForest.MeasureForestBuilder;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.options.IQueryOption;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.cube.AdhocSubQuery;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.table.AdhocTableUnsafe;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.jooq.SQLDialect;

/**
 * Default implementation of {@link IQueryPreparator}.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
@RequiredArgsConstructor
public class StandardQueryPreparator implements IQueryPreparator {

	// By default, the filters are not modified
	@NonNull
	@Default
	final IImplicitFilter implicitFilter = query -> ISliceFilter.MATCH_ALL;

	@NonNull
	@Default
	final IImplicitOptions implicitOptions = query -> ImmutableSet.of();

	@NonNull
	@Default
	final ListeningExecutorService concurrentExecutorService = AdhocUnsafe.adhocCommonPool;

	// If not-concurrent, we want the simpler execution plan as possible
	// it is especially important to simplify thread-jumping lowering readability of stack-traces
	// TODO Should we anyway execute queries in some Adhoc-thread ? It may help monitoring CPU-activity and
	// RAM-allocations
	@NonNull
	@Default
	final ListeningExecutorService nonConcurrentExecutorService = MoreExecutors.newDirectExecutorService();

	// Dedicated pool for external database queries (e.g. DuckDB), to avoid blocking CPU threads on I/O.
	// Configurable per CubeWrapper to allow different tables to use different DB pools.
	@NonNull
	@Default
	final ListeningExecutorService dbExecutorService = MoreExecutors.newDirectExecutorService();

	@NonNull
	@Default
	IQueryStepCache queryStepCache = IQueryStepCache.noCache();

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
				.dbExecutorService(getDBExecutorService(table, preparedQuery))
				.queryStepCache(getQueryStepCache(preparedQuery))
				.sliceFactory(AdhocFactoriesUnsafe.factories.getSliceFactoryFactory().makeFactory(preparedQuery))
				.build();

		// Filtering the forest is useful for edge-cases like:
		// - columnGenerator: we should consider only measures in the queryPlan
		IMeasureForest relevantForest =
				filterForest(fullQueryPod, preparedQuery).name(forest.getName() + "-filtered").build();

		return fullQueryPod.toBuilder().forest(relevantForest).build();
	}

	protected ListeningExecutorService getDBExecutorService(ITableWrapper table, ICubeQuery preparedQuery) {
		if (table instanceof JooqTableWrapper jooqTableWrapper && jooqTableWrapper.getTableParameters().getDslSupplier().getDSLContext().dialect() == SQLDialect.DUCKDB) {
			// DuckDB is very efficient at parallelization: we should not submit many queries concurrently to it
			return AdhocTableUnsafe.adhocDbPool;
		} else {
			// By default, we rely on the configured pool, which is by default a directExecutor
			return dbExecutorService;
		}
	}

	protected IQueryStepCache getQueryStepCache(ICubeQuery preparedQuery) {
		if (preparedQuery.getOptions().contains(StandardQueryOptions.NO_CACHE)) {
			return IQueryStepCache.noCache();
		} else {
			return queryStepCache;
		}
	}

	/**
	 * 
	 * @param forest
	 * @param preparedQuery
	 * @return a {@link MeasureForest} restricted to the measures playing a role in {@link ICubeQuery}.
	 */
	protected MeasureForestBuilder filterForest(IMeasureResolver forest, ICubeQuery preparedQuery) {
		Set<IMeasure> relevantMeasures = new LinkedHashSet<>();

		addSubForest(forest, preparedQuery, relevantMeasures);
		addColumnGenerators(forest, relevantMeasures);

		return MeasureForest.builder().measures(relevantMeasures);
	}

	protected void addColumnGenerators(IMeasureResolver forest, Set<IMeasure> relevantMeasures) {
		// Add all IMayHaveColumnGenerator independantly of the measures
		// This is useful to make measures like `COUNT(*)` functional (instead of throwing `UnknownMeasure`)
		if (forest instanceof IHasMeasures hasMeasures) {
			hasMeasures.getMeasures()
					.stream()
					.filter(IMayHaveColumnGenerator.class::isInstance)
					.map(IMayHaveColumnGenerator.class::cast)
					.forEach(mayHaveColumngenerator -> {
						relevantMeasures.add((IMeasure) mayHaveColumngenerator);
					});
		} else {
			throw new IllegalArgumentException(
					"%s does not implements %s".formatted(forest.getClass().getName(), IHasMeasures.class.getName()));
		}
	}

	@SuppressWarnings("PMD.CollapsibleIfStatements")
	protected void addSubForest(IMeasureResolver forest, ICubeQuery preparedQuery, Set<IMeasure> relevantMeasures) {
		Set<IMeasure> measuresToAdd = new LinkedHashSet<>(preparedQuery.getMeasures());
		while (!measuresToAdd.isEmpty()) {
			Set<String> nextMeasuresToAdd = new LinkedHashSet<>();

			measuresToAdd.forEach(measureToAdd -> {
				IMeasure resolvedMeasure = forest.resolveIfRef(measureToAdd);

				if (relevantMeasures.add(resolvedMeasure)) {

					// BEWARE Not `IHasUnderlyingNames` as it is informative. e.g. in Composite Cube, measure may
					// list
					// underlying measures in underlying cubes, while in compositeCube.queryPlan, these are
					// irrelevant.
					if (resolvedMeasure instanceof IHasUnderlyingMeasures hasUnderlyings) {
						nextMeasuresToAdd.addAll(hasUnderlyings.getUnderlyingNames());
					}
				}
			});

			measuresToAdd.clear();
			measuresToAdd.addAll(
					nextMeasuresToAdd.stream().map(m -> forest.resolveIfRef(ReferencedMeasure.ref(m))).toList());
		}
	}

	protected ListeningExecutorService getExecutorService(ICubeQuery preparedQuery) {
		if (StandardQueryOptions.CONCURRENT.isActive(preparedQuery.getOptions())
				|| StandardQueryOptions.NON_BLOCKING.isActive(preparedQuery.getOptions())) {
			// Concurrent query: execute in a dedicated pool
			return concurrentExecutorService;
		} else {
			// Not concurrent query: rely on current thread
			return nonConcurrentExecutorService;
		}
	}

	protected ICubeQuery combineWithImplicit(ICubeQuery rawQuery) {
		ISliceFilter preprocessedFilter =
				FilterBuilder.and(rawQuery.getFilter(), implicitFilter.getImplicitFilter(rawQuery)).optimize();

		Set<IQueryOption> addedOptions = implicitOptions.getOptions(rawQuery);
		CubeQuery query = CubeQuery.edit(rawQuery).filter(preprocessedFilter).options(addedOptions).build();

		if (rawQuery instanceof AdhocSubQuery subQuery) {
			return subQuery.toBuilder().subQuery(query).build();
		} else {
			return query;
		}
	}

}
