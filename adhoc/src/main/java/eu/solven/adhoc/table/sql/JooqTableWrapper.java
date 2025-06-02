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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.InvalidResultException;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.SuppliedTabularRecordStream;
import eu.solven.adhoc.data.row.TabularRecordOverMaps;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.IJooqTableQueryFactory.QueryWithLeftover;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters.JooqTableWrapperParametersBuilder;
import eu.solven.adhoc.table.sql.duckdb.DuckDbHelper;
import eu.solven.adhoc.table.transcoder.AdhocTranscodingHelper;
import eu.solven.adhoc.table.transcoder.IdentityImplicitTranscoder;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Wraps a {@link Connection} and rely on JooQ to use it as database for {@link TableQuery}.
 *
 * @author Benoit Lacelle
 */
@Builder
@RequiredArgsConstructor
@Slf4j
@ToString(of = "name")
public class JooqTableWrapper implements ITableWrapper {
	// TODO Investigate the benefit of String internalization
	// May be propagated into a more general dictionarization
	private static final boolean internStrings = false;

	@NonNull
	final String name;

	@NonNull
	final JooqTableWrapperParameters tableParameters;

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Collection<ColumnMetadata> getColumns() {
		List<Field<?>> fields = getFields();

		// https://duckdb.org/docs/sql/expressions/star.html
		Map<String, ColumnMetadata> columnToType = new HashMap<>();

		fields.forEach(field -> {
			String fieldName = field.getName();

			Class<?> fieldType = field.getType();
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

	protected List<Field<?>> getFields() {
		Field<?>[] fields;

		try {
			fields = getResultForFields()
					.fields();
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
	 *
	 * @return a {@link Result} which can be used to fetch the fields of this table.
	 */
	protected Result<Record> getResultForFields() {
		return tableParameters.getDslSupplier()
				.getDSLContext()
				.select()
				.from(tableParameters.getTable())
				.limit(0).fetch();
	}

	public static JooqTableWrapper newInstance(Map<String, ?> options) {
		JooqTableWrapperParametersBuilder parametersBuilder = JooqTableWrapperParameters.builder();

		String tableName;
		if (options.containsKey("tableName")) {
			tableName = MapPathGet.getRequiredString(options, "tableName");
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
	public ITabularRecordStream streamSlices(QueryPod queryPod, TableQueryV2 tableQuery) {
		if (queryPod.getTable() != this) {
			throw new IllegalStateException("Inconsistent tables: %s vs %s".formatted(queryPod.getTable(), this));
		}

		IJooqTableQueryFactory queryFactory = makeQueryFactory();

		IJooqTableQueryFactory.QueryWithLeftover resultQuery = queryFactory.prepareQuery(tableQuery);

		if (tableQuery.isExplain() || tableQuery.isDebug()) {
			log.info("[EXPLAIN] SQL to db={}: `{}` and lateFilter={}",
					getName(),
					resultQuery.getQuery().getSQL(ParamType.INLINED),
					resultQuery.getLeftover());
		}
		if (tableQuery.isDebug()) {
			debugResultQuery(resultQuery);
		}

		Stream<ITabularRecord> tableStream = toMapStream(resultQuery);

		Spliterator<ITabularRecord> originalSpliterator = tableStream.spliterator();
		// Given the groupBy, we are guaranteed to receive distinct records
		int modifiedCharacteristics = originalSpliterator.characteristics() | Spliterator.DISTINCT;
		Stream<ITabularRecord> modifiedStream =
				StreamSupport.stream(() -> originalSpliterator, modifiedCharacteristics, false);

		boolean distinctSlices = areDistinctSliced(tableQuery, resultQuery);

		return new SuppliedTabularRecordStream(tableQuery, distinctSlices, () -> modifiedStream);
	}

	protected boolean areDistinctSliced(TableQueryV2 tableQuery, QueryWithLeftover resultQuery) {
		if (resultQuery.getLeftover().isMatchAll()) {
			return true;
		} else {
			// We may have queried columns which are not part of the groupBy
			// TODO We may return true if we know the lateFiltered columns are also in the groupBy
			return false;
		}
	}

	protected IJooqTableQueryFactory makeQueryFactory() {
		DSLContext dslContext = makeDsl();

		return makeQueryFactory(dslContext);
	}

	protected void debugResultQuery(IJooqTableQueryFactory.QueryWithLeftover resultQuery) {
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
				.operatorsFactory(tableParameters.getOperatorsFactory())
				.table(tableParameters.getTable())
				.dslContext(dslContext)
				.build();
	}

	protected Stream<ITabularRecord> toMapStream(IJooqTableQueryFactory.QueryWithLeftover sqlQuery) {
		return sqlQuery.getQuery().stream().map(r -> intoMap(sqlQuery.getFields(), r)).filter(row -> {
			return AdhocTranscodingHelper.match(new IdentityImplicitTranscoder(), sqlQuery.getLeftover(), row);
		});

	}

	// Inspired from AbstractRecord.intoMap
	// Take original `queriedColumns` as the record may not clearly express aliases (e.g. `p.name` vs `name`). And it
	// is ambiguous to build a `columnName` from a `Name`.
	protected ITabularRecord intoMap(AggregatedRecordFields fields, Record r) {
		Map<String, Object> aggregates;

		int columnShift = 0;

		List<String> aggregateFields = fields.getAggregates();
		{
			int size = aggregateFields.size();
			aggregates = LinkedHashMap.newLinkedHashMap(size);

			for (int i = 0; i < size; i++) {
				String columnName = aggregateFields.get(i);

				Object value = r.get(columnShift + i);
				if (value != null) {
					if (internStrings && value instanceof String string) {
						// We argue that given coordinate will be generated many times by the application:
						// We'd like to enable reference-check on it
						value = string.intern();
					}

					Object previousValue = aggregates.put(columnName, value);
					if (previousValue != null) {
						throw new InvalidResultException("Field " + columnName + " is not unique in Record : " + r);
					}
				}
			}
		}

		Map<String, Object> slice;
		columnShift += fields.getAggregates().size();
		{
			List<String> groupByFields = fields.getColumns();
			int size = groupByFields.size();

			slice = LinkedHashMap.newLinkedHashMap(size);
			for (int i = 0; i < size; i++) {
				String columnName = groupByFields.get(i);

				Object previousValue = slice.put(columnName, r.get(columnShift + i));
				if (previousValue != null) {
					throw new InvalidResultException("Field " + columnName + " is not unique in Record : " + r);
				}
			}
		}

		columnShift += fields.getColumns().size();
		{
			List<String> groupByFields = fields.getLateColumns();
			int size = groupByFields.size();

			for (int i = 0; i < size; i++) {
				String columnName = groupByFields.get(i);

				Object previousValue = slice.put(columnName, r.get(columnShift + i));
				if (previousValue != null) {
					throw new InvalidResultException("Field " + columnName + " is not unique in Record : " + r);
				}
			}
		}

		return TabularRecordOverMaps.builder().aggregates(aggregates).slice(slice).build();
	}

	@Override
	public CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		if (SQLDialect.DUCKDB.equals(tableParameters.getDslSupplier().getDSLContext().dialect())) {
			return DuckDbHelper.getCoordinates(this, column, valueMatcher, limit);
		} else {
			return ITableWrapper.super.getCoordinates(column, valueMatcher, limit);
		}
	}

	@Override
	public Map<String, CoordinatesSample> getCoordinates(Map<String, IValueMatcher> columnToValueMatcher, int limit) {
		// TODO How should `null` be reported?
		if (SQLDialect.DUCKDB.equals(tableParameters.getDslSupplier().getDSLContext().dialect())) {
			return DuckDbHelper.getCoordinates(this, columnToValueMatcher, limit);
		} else {
			return ITableWrapper.super.getCoordinates(columnToValueMatcher, limit);
		}
	}

}
