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
package eu.solven.adhoc.cube;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import org.jspecify.annotations.NonNull;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ForwardingListenableFuture.SimpleForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.column.generated_column.ColumnGeneratorHelpers;
import eu.solven.adhoc.column.generated_column.IColumnGenerator;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.ICubeQueryEngine;
import eu.solven.adhoc.engine.context.IQueryPreparator;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.context.StandardQueryPreparator;
import eu.solven.adhoc.engine.observability.IHasHealthDetails;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.engine.step.ICubeQuery;
import eu.solven.adhoc.eventbus.IAdhocEventBus;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.filter.value.InMatcher;
import eu.solven.adhoc.measure.forest.IMeasureForest;
import eu.solven.adhoc.measure.forest.MeasureForest;
import eu.solven.adhoc.measure.operator.IHasOperatorFactory;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.model.measure.IMeasure;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.transcoder.AliasingContext;
import eu.solven.adhoc.util.IHasCache;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Combines an {@link CubeQueryEngine}, including its {@link MeasureForest} and a {@link ITableWrapper}.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder(toBuilder = true)
@Slf4j
public class CubeWrapper implements ICubeWrapper, IHasHealthDetails, IHasCache {
	@NonNull
	@Builder.Default
	@Getter
	final String name = "someCubeName";

	// Execute the query
	@NonNull
	@Default
	final ICubeQueryEngine engine = CubeQueryEngine.builder().build();
	// Holds the data
	@NonNull
	final ITableWrapper table;
	// Holds the indicators definitions
	@NonNull
	final IMeasureForest forest;
	// Enable transcoding from table to cube
	@NonNull
	@Default
	final IColumnsManager columnsManager = ColumnsManager.builder().build();
	// Wrap a query (e.g. with queryId, implicitFilter, etc)
	@NonNull
	@Default
	final IQueryPreparator queryPreparator = StandardQueryPreparator.builder().build();

	// Lazy / memoised column-resolution subsystem. Caching the result dedupes log warnings about discarded aliases
	// (one warning per cube lifetime instead of per call) and avoids recomputing on large cubes. The reference is
	// final; invalidation is handled by `CubeColumnsWrapper.invalidateAll()` which swaps its internal memo.
	@Getter(AccessLevel.NONE)
	final Supplier<CubeColumnsWrapper> columns = Suppliers.memoize(this::makeColumnsWrapper);

	protected CubeColumnsWrapper makeColumnsWrapper() {
		return new CubeColumnsWrapper(table, columnsManager, forest, engine, name);
	}

	@Override
	public Map<String, IMeasure> getNameToMeasure() {
		return forest.getNameToMeasure();
	}

	@Override
	public ITabularView execute(ICubeQuery query) {
		// Input query may be BLOCKING or NON_BLOCKING
		QueryPod queryPod = queryPreparator.prepareQuery(table, forest, columnsManager, query);
		return engine.execute(queryPod);
	}

	@Override
	public ListenableFuture<ITabularView> executeAsync(ICubeQuery query) {
		if (query.getOptions().contains(StandardQueryOptions.BLOCKING)) {
			throw new IllegalArgumentException("Can not asyncExecute() a BLOCKING query. Was %s".formatted(query));
		} else if (!query.getOptions().contains(StandardQueryOptions.NON_BLOCKING)) {
			// Ensure NON_BLOCKING for queryPod creation
			query = CubeQuery.edit(query).options(query.getOptions()).option(StandardQueryOptions.NON_BLOCKING).build();
		}

		QueryPod queryPod = queryPreparator.prepareQuery(table, forest, columnsManager, query);
		ListenableFuture<ITabularView> future =
				Futures.submit(() -> engine.execute(queryPod), queryPod.getExecutorService());

		// Wrap to enable cancellation to be propagated to the queryPod
		return new SimpleForwardingListenableFuture<>(future) {

			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				// Cancelling the future shall cancel the queryPod, which may itself cancel the inner parts of the query
				queryPod.cancel();

				return super.cancel(mayInterruptIfRunning);
			}
		};

	}

	@Override
	public Collection<ColumnMetadata> getColumns() {
		return columns.get().getColumns();
	}

	/**
	 * Drops the cached column metadata so the next {@link #getColumns()} re-computes. Also propagates to the underlying
	 * {@link ITableWrapper} when it is itself an {@link IHasCache}, so a downstream schema change is picked up without
	 * callers having to invalidate every layer manually.
	 */
	@Override
	public void invalidateAll() {
		columns.get().invalidateAll();
		if (table instanceof IHasCache tableCache) {
			tableCache.invalidateAll();
		}
	}

	/**
	 * Lombok @Builder
	 *
	 * @author Benoit Lacelle
	 */
	// Builder fields populated via chained setters before .build(); NullAway can't see the cross-method init.
	@SuppressWarnings("NullAway.Init")
	public static class CubeWrapperBuilder {
		public CubeWrapperBuilder eventBus(IAdhocEventBus eventBus) {
			// BEWARE Is this the proper way the ensure the eventBus is written in proper places?
			ColumnsManager columnsManager = (ColumnsManager) this.build().getColumnsManager();
			this.columnsManager(columnsManager.toBuilder().eventBus(eventBus).build());

			return this;
		}
	}

	@Override
	public Map<String, CoordinatesSample> getCoordinates(Map<String, IValueMatcher> columnToValueMatcher, int limit) {
		IOperatorFactory operatorFactory = IHasOperatorFactory.getOperatorsFactory(engine);

		List<IColumnGenerator> columnGenerators = columnsManager.getGeneratedColumns(operatorFactory,
				forest.getMeasures(),
				InMatcher.matchIn(columnToValueMatcher.keySet()));

		Set<String> generatedColumns = columnGenerators.stream()
				.flatMap(cg -> cg.getColumnTypes().keySet().stream())
				.collect(ImmutableSet.toImmutableSet());

		Map<String, CoordinatesSample> columnToSample = new TreeMap<>();

		// Given columns are defined by a measure, not by the table
		for (String generatedColumn : generatedColumns) {
			if (columnToValueMatcher.containsKey(generatedColumn)) {
				IValueMatcher valueMatcher = Objects.requireNonNull(columnToValueMatcher.get(generatedColumn));
				CoordinatesSample coordinates =
						ColumnGeneratorHelpers.getCoordinates(columnGenerators, generatedColumn, valueMatcher, limit);
				columnToSample.put(generatedColumn, coordinates);
			}
		}

		// TODO What if a column is both from table and generated? Trying to get coordinates from an
		// unknown column would generally lead to an error.
		{

			// Can not rely on `table.getColumns().containsKey(column)` as many table have various ways to match a
			// column
			// e.g. `someColumn` and `p.someColumn` may match the same column, while it is unclear to us how to return
			// `p.someColumn` as a column from JooQ
			Map<String, IValueMatcher> tableColumnToValueMatcher = new LinkedHashMap<>();

			AliasingContext transcodedContext = columnsManager.openTranscodingContext();
			Sets.difference(columnToValueMatcher.keySet(), generatedColumns).forEach(cubeColumn -> {
				String tableColumn = transcodedContext.underlyingNonNull(cubeColumn);
				tableColumnToValueMatcher.put(tableColumn, columnToValueMatcher.get(cubeColumn));
			});

			Map<String, CoordinatesSample> tableCoordinates = table.getCoordinates(tableColumnToValueMatcher, limit);

			tableCoordinates.forEach((tableColumn, sample) -> {
				transcodedContext.queried(tableColumn)
						.forEach(queriedColumn -> columnToSample.put(queriedColumn, sample));
			});
		}

		return columnToSample;
	}

	@Override
	public Map<String, ?> getHealthDetails() {
		Map<String, Object> details = new LinkedHashMap<>();

		Map<String, Object> tableDetails = new LinkedHashMap<>();
		tableDetails.put("name", table.getName());
		tableDetails.put("type", table.getClass().getName());

		if (table instanceof IHasHealthDetails tableHasHealth) {
			tableDetails.putAll(tableHasHealth.getHealthDetails());
		}

		details.put("table", tableDetails);

		return details;
	}

	public static Map<String, Object> makeDetails(ICubeWrapper cube) {
		Map<String, Object> details = new LinkedHashMap<>();
		details.put("columns", cube.columnsKeySet().size());
		details.put("measures", cube.getMeasures().size());

		if (cube instanceof IHasHealthDetails hasHealthDetails) {
			details.putAll(hasHealthDetails.getHealthDetails());
		}
		return details;
	}

}
