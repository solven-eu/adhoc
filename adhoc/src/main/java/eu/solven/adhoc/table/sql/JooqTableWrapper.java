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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.InvalidResultException;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.dag.ExecutingQueryContext;
import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.SuppliedTabularRecordStream;
import eu.solven.adhoc.data.row.TabularRecordOverMaps;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters.JooqTableWrapperParametersBuilder;
import eu.solven.adhoc.table.sql.duckdb.DuckDbHelper;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Wraps a {@link Connection} and rely on JooQ to use it as database for {@link TableQuery}.
 *
 * @author Benoit Lacelle
 */
@AllArgsConstructor
@Slf4j
@ToString(of = "name")
public class JooqTableWrapper implements ITableWrapper {

	final String name;

	final JooqTableWrapperParameters dbParameters;

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Map<String, Class<?>> getColumns() {
		Field<?>[] select;

		try {
			select = dbParameters.getDslSupplier()
					.getDSLContext()
					.select()
					.from(dbParameters.getTable())
					.limit(0)
					.fetch()
					.fields();
		} catch (DataAccessException e) {
			if (e.getMessage().contains("IO Error: No files found that match the pattern")) {
				if (log.isDebugEnabled()) {
					log.warn("No column for table=`{}` due to missing files", getName(), e);
				} else {
					// The failure may be missing anywhere in the SQL (e.g. the main `FROM`, or any `JOIN`)
					log.warn("No column for table=`{}` due to missing files. sqlMsg={}", getName(), e.getMessage());
				}
				return Collections.emptyMap();
			} else {
				throw e;
			}
		}

		// https://duckdb.org/docs/sql/expressions/star.html

		Map<String, Class<?>> columnToType = new LinkedHashMap<>();

		Stream.of(select).forEach(field -> {
			String fieldName = field.getName();

			Class<?> fieldType = field.getType();
			Class<?> previousType = columnToType.put(fieldName, fieldType);
			if (previousType != null) {
				log.debug("Multiple columns with same name. Typically happens on a JOIN");
				if (!Objects.equals(fieldType, previousType)) {
					log.warn("Multiple columns with same name (table=%s column=%s), and different types: %s != %s",
							getName(),
							fieldName,
							previousType,
							fieldType);
				}
			}
		});

		return columnToType;
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
		return dbParameters.getDslSupplier().getDSLContext();
	}

	@Override
	public ITabularRecordStream streamSlices(ExecutingQueryContext executingQueryContext, TableQuery tableQuery) {
		if (executingQueryContext.getTable() != this) {
			throw new IllegalStateException(
					"Inconsistent tables: %s vs %s".formatted(executingQueryContext.getTable(), this));
		}

		IJooqTableQueryFactory queryFactory = makeQueryFactory();

		ResultQuery<Record> resultQuery = queryFactory.prepareQuery(tableQuery);

		if (tableQuery.isExplain() || tableQuery.isDebug()) {
			log.info("[EXPLAIN] SQL to db={}: `{}`", getName(), resultQuery.getSQL(ParamType.INLINED));
			// resultQuery.fields()
		}
		if (tableQuery.isDebug()) {
			debugResultQuery(resultQuery);
		}

		AggregatedRecordFields fields = queryFactory.makeSelectedColumns(tableQuery);

		Stream<ITabularRecord> tableStream = toMapStream(fields, resultQuery);

		return new SuppliedTabularRecordStream(tableQuery, () -> tableStream);
	}

	protected IJooqTableQueryFactory makeQueryFactory() {
		DSLContext dslContext = makeDsl();

		IJooqTableQueryFactory queryFactory = makeQueryFactory(dslContext);
		return queryFactory;
	}

	protected void debugResultQuery(ResultQuery<Record> resultQuery) {
		DSLContext dslContext = makeDsl();

		try {
			Map<String, Class<?>> columnNameToType = getColumns();
			log.info("[DEBUG] {}", columnNameToType);

			// https://stackoverflow.com/questions/76908078/how-to-check-if-a-query-is-valid-or-not-using-jooq
			String plan = dslContext.explain(resultQuery).plan();

			log.info("[DEBUG] Query plan: {}{}", System.lineSeparator(), plan);
		} catch (RuntimeException e) {
			log.warn("[DEBUG] Issue on EXPLAIN on query={}", resultQuery, e);
		}
	}

	protected IJooqTableQueryFactory makeQueryFactory(DSLContext dslContext) {
		return JooqTableQueryFactory.builder()
				.operatorsFactory(dbParameters.getOperatorsFactory())
				.table(dbParameters.getTable())
				.dslContext(dslContext)
				.build();
	}

	protected Stream<ITabularRecord> toMapStream(AggregatedRecordFields fields, ResultQuery<Record> sqlQuery) {
		return sqlQuery.stream().map(r -> intoMap(fields, r));

	}

	// Inspired from AbstractRecord.intoMap
	// Take original `queriedColumns` as the record may not clearly expresses aliases (e.g. `p.name` vs `name`). And it
	// is ambiguous to build a `columnName` from a `Name`.
	protected ITabularRecord intoMap(AggregatedRecordFields fields, Record r) {
		Map<String, Object> aggregates;

		List<String> aggregateFields = fields.getAggregates();
		{
			int size = aggregateFields.size();
			aggregates = new LinkedHashMap<>(size);

			for (int i = 0; i < size; i++) {
				String columnName = aggregateFields.get(i);

				Object previousValue = aggregates.put(columnName, r.get(i));
				if (previousValue != null) {
					throw new InvalidResultException("Field " + columnName + " is not unique in Record : " + r);
				}
			}
		}

		Map<String, Object> groupBys;
		{
			List<String> groupByFields = fields.getColumns();
			int size = groupByFields.size();

			groupBys = new LinkedHashMap<>(size);
			for (int i = 0; i < size; i++) {
				String columnName = groupByFields.get(i);

				Object previousValue = groupBys.put(columnName, r.get(aggregateFields.size() + i));
				if (previousValue != null) {
					throw new InvalidResultException("Field " + columnName + " is not unique in Record : " + r);
				}
			}
		}

		return TabularRecordOverMaps.builder().aggregates(aggregates).groupBys(groupBys).build();
	}

	protected String toQualifiedName(Field<?> field) {
		// field.getQualifiedName()
		return Stream.of(field.getQualifiedName().parts()).map(Name::first).collect(Collectors.joining("."));
	}

	@Override
	public CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		if (SQLDialect.DUCKDB.equals(dbParameters.getDslSupplier().getDSLContext().dialect())) {
			return DuckDbHelper.getCoordinates(this, column, valueMatcher, limit);
		} else {
			return ITableWrapper.super.getCoordinates(column, valueMatcher, limit);
		}
	}

	@Override
	public Map<String, CoordinatesSample> getCoordinates(Map<String, IValueMatcher> columnToValueMatcher, int limit) {
		// TODO How should `null` be reported?
		if (SQLDialect.DUCKDB.equals(dbParameters.getDslSupplier().getDSLContext().dialect())) {
			return DuckDbHelper.getCoordinates(this, columnToValueMatcher, limit);
		} else {
			return ITableWrapper.super.getCoordinates(columnToValueMatcher, limit);
		}
	}

}
