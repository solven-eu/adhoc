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
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
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
public class CubeWrapper implements ICubeWrapper, IHasHealthDetails {
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
		Map<String, ColumnMetadata> columnToType = getColumnsWithoutAliases();

		AliasingContext transcodingContext = getColumnsManager().openTranscodingContext();

		// Register aliases in the `alias` field of metadata
		// TODO This does not handle recursive aliases
		getColumnsManager().getColumnAliases().forEach(columnAlias -> {
			String tableName = transcodingContext.underlying(columnAlias);

			ColumnMetadata originalMetadata = columnToType.get(tableName);

			if (originalMetadata == null) {
				// Typically happens on JOINs
				// SQL engines generally returns unqualified columnName
				// Hence, `joinA.columnC` is returned as `columnC` by SQL.
				// But `columnC` is an alias for `joinA.columnC` according to the aliaser.
				// In most situations, we prefer to fallback on the alias, as the project alias is often the same as the
				// SQL alias (i.e. unqualified columnName).
				originalMetadata = columnToType.get(columnAlias);
			}

			if (originalMetadata == null && tableName != null) {
				// Third-try: a JooqTableSupplierBuilder-style aliaser declares `aliasedColor -> b.color`, but the
				// table only knows the unqualified `color` (see JooqTableWrapper.getColumns: SQL backends return
				// column names without their table-qualifier when listing columns). Strip the qualifier and the
				// surrounding jOOQ quotes (e.g. `"b"."color"` → `color`) so the bare lookup matches the table's
				// column names. CubeWrapper sits above the SQL layer and does not own a {@code Parser}, hence
				// this string-level handling rather than the dialect-aware
				// {@code AdhocJooqHelper.unqualifiedColumnName(...)}; consequence: column names that contain
				// unquoted dots are NOT supported here. Such cases need a parser-aware path (likely surfacing
				// the original as a structured {@code Name} from the supplier rather than a String).
				String unqualified = stripQualifierAndQuotes(tableName);
				if (!unqualified.equals(tableName)) {
					originalMetadata = columnToType.get(unqualified);
				}
			}

			if (originalMetadata == null) {
				// Discard: a shared ColumnsManager may carry aliases relevant only to a subset of cubes, so an alias
				// with no underlying column on this cube is not necessarily a bug — but still worth warning about.
				log.warn("Discarding alias={} (tablename={}) as it has no underlying column on cube={}",
						columnAlias,
						tableName,
						getName());
			} else {
				columnToType.put(originalMetadata.getName(), originalMetadata.toBuilder().alias(columnAlias).build());
			}
		});

		// Duplicate each column given its alias
		Map<String, ColumnMetadata> aliasToColumn = new LinkedHashMap<>();
		columnToType.forEach((column, metadata) -> {
			metadata.getAliases().forEach(alias -> {
				aliasToColumn.put(alias, metadata.toBuilder().name(alias).alias(column).build());
			});
		});

		columnToType.putAll(aliasToColumn);

		return columnToType.values();
	}

	/**
	 * String-level qualifier stripper used by {@link #getColumns()}. Removes the segment before the last unquoted dot,
	 * then unwraps surrounding double-quotes — covers the two shapes the supplier can hand us: bare-dotted
	 * ({@code b.color}) and jOOQ-escaped two-part ({@code "b"."color with space"}). Does NOT support column names
	 * containing unquoted dots, since this layer has no parser; for that, plumb a dialect-aware path instead (see the
	 * call site comment).
	 */
	private static String stripQualifierAndQuotes(String qualifiedName) {
		int lastDot = qualifiedName.lastIndexOf('.');
		String unqualified;
		if (lastDot >= 0 && lastDot < qualifiedName.length() - 1) {
			unqualified = qualifiedName.substring(lastDot + 1);
		} else {
			unqualified = qualifiedName;
		}
		if (unqualified.length() >= 2 && unqualified.charAt(0) == '"'
				&& unqualified.charAt(unqualified.length() - 1) == '"') {
			unqualified = unqualified.substring(1, unqualified.length() - 1);
		}
		return unqualified;
	}

	protected Map<String, ColumnMetadata> getColumnsWithoutAliases() {
		Map<String, ColumnMetadata> columnToType = new LinkedHashMap<>();

		// First, register table columns
		table.getColumns().forEach((table) -> {
			columnToType.put(table.getName(), table);
		});

		// Then, register calculated columns (e.g. based on an expression)
		getColumnsManager().getColumnTypes().forEach((columnName, type) -> {
			columnToType.put(columnName,
					ColumnMetadata.builder().name(columnName).tag("calculated").type(type).build());
		});

		IOperatorFactory operatorFactory = IHasOperatorFactory.getOperatorsFactory(engine);
		forest.getMeasures().forEach(measure -> {
			try {
				ColumnGeneratorHelpers
						.getColumnGenerators(operatorFactory, ImmutableSet.of(measure), IValueMatcher.MATCH_ALL)
						.forEach(columnGenerator -> {
							// TODO How conflicts should be handled? `ColumnMetadata.merge`?
							columnGenerator.getColumnTypes().forEach((columnName, type) -> {
								columnToType.put(columnName,
										ColumnMetadata.builder().name(columnName).tag("generated").type(type).build());
							});
						});
			} catch (RuntimeException e) {
				if (AdhocUnsafe.isFailFast()) {
					String msg = "Issue looking for an %s in m=%s c=%s"
							.formatted(IColumnGenerator.class.getSimpleName(), measure, this.getName());
					throw new IllegalStateException(msg, e);
				} else {
					log.warn("Issue looking for an {} in m={} c={}",
							IColumnGenerator.class.getSimpleName(),
							measure,
							this.getName(),
							e);
				}
			}
		});
		return columnToType;
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
