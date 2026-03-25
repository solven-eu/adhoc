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
package eu.solven.adhoc.table.sql;

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.ResultQuery;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.InvalidResultException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.column.ICalculatedColumn;
import eu.solven.adhoc.dataframe.filter.MoreFilterHelpers;
import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.dataframe.row.ITabularRecordFactory;
import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.dataframe.row.SuppliedTabularRecordStream;
import eu.solven.adhoc.dataframe.row.TabularRecordBuilder;
import eu.solven.adhoc.dataframe.row.TabularRecordOverMaps;
import eu.solven.adhoc.engine.cancel.CancellationHelpers;
import eu.solven.adhoc.engine.cancel.CancelledQueryException;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.observability.IHasHealthDetails;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters.JooqTableWrapperParametersBuilder;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.IHasCache;
import eu.solven.adhoc.util.map.AdhocMapPathGet;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Wraps a {@link Connection} and rely on JooQ to use it as database for {@link TableQuery}.
 *
 * @author Benoit Lacelle
 */
@Builder
@AllArgsConstructor
@Slf4j
@ToString(of = "name")
@SuppressWarnings("PMD.GodClass")
public class JooqTableWrapper implements ITableWrapper, IHasCache, IHasHealthDetails {

	@NonNull
	final String name;

	@NonNull
	final JooqTableWrapperParameters tableParameters;

	final LoadingCache<Object, List<Field<?>>> fieldsCache = CacheBuilder.newBuilder()
			// https://github.com/google/guava/wiki/cachesexplained#refresh
			.refreshAfterWrite(Duration.ofMinutes(1))
			.removalListener(new RemovalListener<Object, List<Field<?>>>() {

				@Override
				public void onRemoval(RemovalNotification<Object, List<Field<?>>> notification) {
					RemovalCause cause = notification.getCause();
					List<Field<?>> removedFields = notification.getValue();
					log.debug("Removing fields for {} due to {} (were {})", getName(), cause, removedFields);
				}
			})
			.build(CacheLoader.asyncReloading(CacheLoader.from(this::noCacheGetFields), AdhocUnsafe.maintenancePool));

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void invalidateAll() {
		fieldsCache.invalidateAll();
	}

	@Override
	public Collection<ColumnMetadata> getColumns() {
		List<Field<?>> fields = getFields();

		// https://duckdb.org/docs/sql/expressions/star.html
		Map<String, ColumnMetadata> columnToType = new LinkedHashMap<>();

		// TODO Qualify columns with table
		// https://duckdbsnippets.com/snippets/204/label-columns-based-on-source-table
		// SELECT
		// COLUMNS(t1.*) AS 't1_\0',
		// COLUMNS(t2.*) AS 't2_\0'
		// FROM range(10) t1
		// JOIN range(10) t2 ON t1.range = t2.range

		fields.forEach(field -> {
			String fieldName = field.getName();

			Class<?> fieldType = getFieldType(field);
			ColumnMetadata previousColumn =
					columnToType.put(fieldName, ColumnMetadata.builder().name(fieldName).type(fieldType).build());
			if (previousColumn != null) {
				log.debug("Multiple columns with same name. Typically happens on a JOIN");
				if (!Objects.equals(fieldType, previousColumn.getType())) {
					log.warn("Multiple columns with same name (table={} column={}), and different types: {} != {}",
							getName(),
							fieldName,
							previousColumn,
							fieldType);
				}
			}
		});

		return columnToType.values();
	}

	protected Class<?> getFieldType(Field<?> field) {
		// Relates with org.duckdb.DuckDBVector.getObject(int)
		Class<?> rawFieldType = field.getType();

		if (java.sql.Date.class.isAssignableFrom(rawFieldType)) {
			// BEWARE Clarify the proper to infer from rawTypes to the actual types expected to be returned by the JDBC
			// driver
			return LocalDate.class;
		} else {
			return rawFieldType;
		}
	}

	protected List<Field<?>> getFields() {
		List<Field<?>> fields = fieldsCache.getUnchecked(Boolean.TRUE);

		if (fields.isEmpty()) {
			// Fields is typically empty if we were missing some files: let's retry
			fieldsCache.invalidateAll();
			fields = fieldsCache.getUnchecked(Boolean.TRUE);
		}

		return fields;
	}

	protected List<Field<?>> noCacheGetFields() {
		Field<?>[] fields;

		try {
			fields = getResultForFields().fields();
		} catch (DataAccessException e) {
			if (e.getMessage().contains("IO Error: No files found that match the pattern")) {
				if (log.isDebugEnabled()) {
					log.warn("No column for table=`{}` due to missing files", getName(), e);
				} else {
					// The failure may be missing anywhere in the SQL (e.g. the main `FROM`, or any `JOIN`)
					log.warn("No column for table=`{}` due to missing files. sqlMsg={}", getName(), e.getMessage());
				}
				return Collections.emptyList();
			} else {
				throw e;
			}
		}
		return Arrays.asList(fields);
	}

	/**
	 * This may be overridden for underlying SQL databases requiring a specific SQL.
	 * 
	 * @return a {@link Result} which can be used to fetch the fields of this table.
	 */
	protected Result<Record> getResultForFields() {
		// Log in INFO as this operation maybe a bit slow
		log.info("Fetching fields of table={}", getName());
		return tableParameters.getDslSupplier()
				.getDSLContext()
				.select()
				.from(tableParameters.getTable())
				.limit(0)
				.fetch();
	}

	public static JooqTableWrapper newInstance(Map<String, ?> options) {
		JooqTableWrapperParametersBuilder parametersBuilder = JooqTableWrapperParameters.builder();

		String tableName;
		if (options.containsKey("tableName")) {
			tableName = AdhocMapPathGet.getRequiredString(options, "tableName");
			parametersBuilder.tableName(tableName);
		} else {
			tableName = "someTableName";
		}

		JooqTableWrapperParameters parameters = parametersBuilder.build();
		return new JooqTableWrapper(tableName, parameters);
	}

	public DSLContext makeDsl() {
		return tableParameters.getDslSupplier().getDSLContext();
	}

	@Override
	public ITabularRecordStream streamSlices(QueryPod queryPod, TableQueryV4 tableQuery) {
		if (!Objects.equals(this, queryPod.getTable())) {
			throw new IllegalStateException("Inconsistent tables: %s vs %s".formatted(queryPod.getTable(), this));
		} else {
			Optional<IAdhocColumn> optInvalidColumn = tableQuery.getGroupBys()
					.stream()
					.flatMap(gb -> gb.getColumns().stream())
					.filter(c -> c instanceof ICalculatedColumn)
					.findAny();
			if (optInvalidColumn.isPresent()) {
				// These should be handled by ColumnsManager, which itself call ITableWrapper
				// BEWARE We still accept TableExpressionColumn
				throw new IllegalArgumentException("%s are not manageable by ITableWrapper. query=%s"
						.formatted(optInvalidColumn.get(), tableQuery));
			}
		}

		IGroupBy mergedGroupBy = GroupByColumns.mergeNonAmbiguous(tableQuery.getGroupBys());

		IJooqTableQueryFactory queryFactory = makeQueryFactory();

		// TODO This should be checked only once. Is it expensive?
		makeDsl().connection(c -> {
			if (c.getAutoCommit()) {
				// Performance check-up
				// BEWARE ClickHouse seems not to allow autoCommit false
				// TODO Switch to WARN once we have a clear strategy to set this flag. Typically, most test-cases have
				// autoCommit=true for now
				log.debug("autoCommit should not be true. connection={}", c);
			}
		});

		QueryWithLeftover resultQuery = queryFactory.prepareQuery(tableQuery);

		if (tableQuery.isDebugOrExplain()) {
			log.info("[EXPLAIN] SQL to db={}: `{}` and lateFilter={}",
					getName(),
					toSQL(resultQuery.getQuery()),
					resultQuery.getLeftover());
		}
		if (tableQuery.isDebug()) {
			debugResultQuery(resultQuery);
		}

		Stream<ITabularRecord> tableStream = toMapStream(queryPod, mergedGroupBy, resultQuery);

		boolean distinctSlices = areDistinctSliced(tableQuery, resultQuery);

		Stream<ITabularRecord> modifiedStream;
		if (distinctSlices) {
			Spliterator<ITabularRecord> originalSpliterator = tableStream.spliterator();

			int modifiedCharacteristics = originalSpliterator.characteristics() | Spliterator.DISTINCT;
			modifiedStream =
					StreamSupport.stream(() -> originalSpliterator, modifiedCharacteristics, tableStream.isParallel());
		} else {
			modifiedStream = tableStream;

		}
		return new SuppliedTabularRecordStream(tableQuery, distinctSlices, () -> modifiedStream);
	}

	protected String toSQL(ResultQuery<Record> resultQuery) {
		return resultQuery.getSQL(ParamType.INLINED);
	}

	protected boolean areDistinctSliced(TableQueryV4 tableQuery, QueryWithLeftover resultQuery) {
		if (resultQuery.getQueries().size() >= 2) {
			// Given the groupBy, we are guaranteed to receive distinct records
			// Clarify when partitioning breaks isDistinct
			return false;
		} else if (resultQuery.getLeftover().isMatchAll() && resultQuery.getAggregatorToLeftovers().isEmpty()) {
			// SQL Engines guarantee a single record per groupBy
			// InMemoryTable does not manage aggregation: how is this managed?
			return true;
		} else {
			// We may have queried columns which are not part of the groupBy
			// TODO We may return true if we know the leftovers columns are also in the groupBy
			return false;
		}
	}

	protected IJooqTableQueryFactory makeQueryFactory() {
		DSLContext dslContext = makeDsl();

		return makeQueryFactory(dslContext);
	}

	protected void debugResultQuery(QueryWithLeftover resultQuery) {
		DSLContext dslContext = makeDsl();

		try {
			Map<String, Class<?>> columnNameToType = getColumnTypes();
			log.info("[DEBUG] {}", columnNameToType);

			// https://stackoverflow.com/questions/76908078/how-to-check-if-a-query-is-valid-or-not-using-jooq
			String plan = dslContext.explain(resultQuery.getQuery()).plan();

			log.info("[DEBUG] Query plan: {}{}", System.lineSeparator(), plan);
			log.info("[DEBUG] Late filter: {}", resultQuery.getLeftover());
		} catch (RuntimeException e) {
			log.warn("[DEBUG] Issue on EXPLAIN on query={}", resultQuery, e);
		}
	}

	protected IJooqTableQueryFactory makeQueryFactory(DSLContext dslContext) {
		return JooqTableQueryFactory.builder()
				.operatorFactory(tableParameters.getOperatorFactory())
				.table(tableParameters.getTable())
				.dslContext(dslContext)
				.build();
	}

	protected Stream<ITabularRecord> toMapStream(QueryPod queryPod,
			IGroupBy mergedGroupBy,
			QueryWithLeftover sqlQuery) {
		Stream<ITabularRecord> tabularRecords = streamTabularRecords(queryPod, mergedGroupBy, sqlQuery);
		return tabularRecords
				// leftover in WHERE
				.filter(row -> MoreFilterHelpers.match(sqlQuery.getLeftover(), row))
				// leftover in FILTER
				.map(row -> applyAggregatorLeftovers(sqlQuery, row));
	}

	protected Stream<ITabularRecord> streamTabularRecords(QueryPod queryPod,
			IGroupBy mergedGroupBy,
			QueryWithLeftover sqlQuery) {
		List<ResultQuery<Record>> resultQuery = sqlQuery.getQueries();

		return resultQuery.stream().flatMap(oneQuery -> {
			ITabularRecordFactory tabularRecordFactory =
					makeTabularRecordFactory(queryPod, mergedGroupBy, sqlQuery, oneQuery);
			return toStream(queryPod, oneQuery).map(r -> intoTabularRecord(tabularRecordFactory, r));
		});
	}

	/**
	 * Applies any aggregator-level leftover filters that could not be pushed into the SQL FILTER clause.
	 */
	protected ITabularRecord applyAggregatorLeftovers(QueryWithLeftover sqlQuery, ITabularRecord row) {
		Map<String, ISliceFilter> aggregatorToLeftovers = sqlQuery.getAggregatorToLeftovers();
		if (aggregatorToLeftovers.isEmpty()) {
			return row;
		} else {
			// Copy aggregates as we may remove if the leftover does not match
			Map<String, ?> aggregates = new LinkedHashMap<>(row.aggregatesAsMap());

			aggregatorToLeftovers.forEach((measure, leftover) -> {
				Object currentValue = aggregates.get(measure);

				if (currentValue == null) {
					// Aggregate is null: no point in checking the leftover on FILTER
					log.trace("Skip removing null aggregate");
				} else if (!MoreFilterHelpers.match(leftover, row)) {
					aggregates.remove(measure);
				}
			});

			return TabularRecordOverMaps.builder()
					.aggregates(aggregates)
					.slice(row.getGroupBy(), row.asSlice())
					.build();
		}
	}

	protected ITabularRecordFactory makeTabularRecordFactory(QueryPod queryPod,
			IGroupBy mergedGroupBy,
			QueryWithLeftover sqlQuery,
			ResultQuery<Record> oneQuery) {
		return JooqTabularRecordFactory.builder()
				.globalGroupBy(mergedGroupBy)
				.fields(sqlQuery.getFields())
				.sliceFactory(queryPod.getSliceFactory())
				.optionalColumns(sqlQuery.getFields().getGroupingColumns())
				.build();
	}

	protected Stream<Record> toStream(QueryPod queryPod, ResultQuery<Record> resultQuery) {
		// BEWARE This cancellation mechanism is quite awkward. JooQ cancellation design seems to have blind spots. We
		// may have to introduce an ExecuteListener to fully handle them.
		if (queryPod.isCancelled()) {
			// Cancel early, before creating the connection/statement
			throw new CancelledQueryException("Query is cancelled");
		}

		// https://github.com/jOOQ/jOOQ/issues/5013
		// https://github.com/jOOQ/jOOQ/issues/19479
		// TODO We should remove this listener once the SQL is fully executed
		Runnable cancellationListener = () -> {
			log.info("Cancelling for queryId={} SQL={}", queryPod.getQueryId().getQueryId(), toSQL(resultQuery));
			resultQuery.cancel();
		};
		queryPod.addCancellationListener(cancellationListener);
		Runnable cancellationListenerOnceStarted = () -> {
			log.info("Cancelling for queryId={} SQL={}", queryPod.getQueryId().getQueryId(), toSQL(resultQuery));
			resultQuery.cancel();
		};

		AtomicBoolean isSecondCancellationListenerRegistered = new AtomicBoolean();

		return resultQuery.fetchSize(this.tableParameters.getStatementFetchSize()).stream().onClose(() -> {
			queryPod.removeCancellationListener(cancellationListener);
			queryPod.removeCancellationListener(cancellationListenerOnceStarted);

			// BEWARE this would not be called in case of exception
			CancellationHelpers.afterCancellable(queryPod);
		}).peek(r -> {
			// BEWARE Registering a second cancellationListener in order to capture cases where the cancellation
			// happened before the statement registration in queryResult
			// BEWARE This does not cover the case of a query being very slow to return a single row. Do we require an
			// ExecutionListener?
			if (isSecondCancellationListenerRegistered.compareAndSet(false, true)) {
				queryPod.addCancellationListener(cancellationListenerOnceStarted);
			}
		});
	}

	// Inspired from AbstractRecord.intoMap
	// Take original `queriedColumns` as the record may not clearly express aliases (e.g. `p.name` vs `name`). And it
	// is ambiguous to build a `columnName` from a `Name`.
	protected ITabularRecord intoTabularRecord(ITabularRecordFactory tabularRecordFactory, Record r) {
		Set<String> absentColumns = tabularRecordFactory.getOptionalColumns().stream().filter(c -> {
			Field<?> groupingField = r.field(JooqTableQueryFactory.groupingAlias(c));

			return !Integer.valueOf(0).equals(groupingField.getValue(r));
		}).collect(ImmutableSet.toImmutableSet());

		TabularRecordBuilder recordBuilder = tabularRecordFactory.makeTabularRecordBuilder(absentColumns);

		int columnShift = 0;

		List<String> aggregateFields = tabularRecordFactory.getAggregates();
		{
			int size = aggregateFields.size();

			for (int i = 0; i < size; i++) {
				Object value = r.get(columnShift + i);
				if (value != null) {
					String columnName = aggregateFields.get(i);
					Object previousValue = recordBuilder.appendAggregate(columnName, value);

					if (previousValue != null) {
						throw new InvalidResultException("Field " + columnName + " is not unique in Record : " + r);
					}
				}
			}

			columnShift += size;
		}

		{
			// Record fields may not match exactly the columns, especially on qualified fields
			ImmutableList<String> columns = tabularRecordFactory.getColumns().asList();
			int size = columns.size();

			int nbToAppend = size - absentColumns.size();
			int nbAppend = 0;

			if (absentColumns.size() != size) {
				for (int i = 0; i < size && nbAppend < nbToAppend; i++) {
					String currentKey = columns.get(i);
					if (absentColumns.contains(currentKey)) {
						log.debug("Skip NULL as {} not in current GROUPING SET", currentKey);
						continue;
					}

					recordBuilder.appendGroupBy(r.get(columnShift + i));
					nbAppend++;
				}
			}
		}

		return recordBuilder.build();
	}

	@Override
	public CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		if (SQLDialect.DUCKDB == tableParameters.getDslSupplier().getDSLContext().dialect()) {
			return DuckDBHelper.getCoordinates(this, column, valueMatcher, limit);
		} else {
			return ITableWrapper.super.getCoordinates(column, valueMatcher, limit);
		}
	}

	@Override
	public Map<String, CoordinatesSample> getCoordinates(Map<String, IValueMatcher> columnToValueMatcher, int limit) {
		// TODO How should `null` be reported?
		if (SQLDialect.DUCKDB == tableParameters.getDslSupplier().getDSLContext().dialect()) {
			return DuckDBHelper.getCoordinates(this, columnToValueMatcher, limit);
		} else {
			return ITableWrapper.super.getCoordinates(columnToValueMatcher, limit);
		}
	}

	@Override
	public Map<String, ?> getHealthDetails() {
		return ImmutableMap.<String, Object>builder()
				.put("tableLike", tableParameters.getTable().toString())
				.put("dialect", tableParameters.getDslSupplier().getDSLContext().dialect())
				.put("dslContextCreationTime", tableParameters.getDslSupplier().getDSLContext().creationTime())
				.build();
	}

}
