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
import org.jooq.conf.ParamType;
import org.jooq.exception.InvalidResultException;

import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.record.AggregatedRecordOverMaps;
import eu.solven.adhoc.record.IAggregatedRecord;
import eu.solven.adhoc.record.IAggregatedRecordStream;
import eu.solven.adhoc.record.SuppliedAggregatedRecordStream;
import eu.solven.adhoc.table.IAdhocTableWrapper;
import eu.solven.adhoc.table.sql.AdhocJooqTableWrapperParameters.AdhocJooqTableWrapperParametersBuilder;
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
public class AdhocJooqTableWrapper implements IAdhocTableWrapper {

	final String name;

	final AdhocJooqTableWrapperParameters dbParameters;

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Map<String, Class<?>> getColumns() {
		Field<?>[] select = dbParameters.getDslSupplier()
				.getDSLContext()
				.select()
				.from(dbParameters.getTable())
				.limit(0)
				.fetch()
				.fields();

		// https://duckdb.org/docs/sql/expressions/star.html

		Map<String, Class<?>> columnToType = new LinkedHashMap<>();

		Stream.of(select).forEach(field -> {
			String fieldName = field.getName();

			// TODO Columns should express queryable columns, not underlying columns
			// dbParameters.getTranscoder();

			Class<?> fieldType = field.getType();
			Class<?> previousType = columnToType.put(fieldName, fieldType);
			if (previousType != null) {
				log.debug("Multiple columns with same name. Typically happens on a JOIN");
				if (!Objects.equals(fieldType, previousType)) {
					log.warn("Multiple columns with same name (name=%s), and different types: %s != %s",
							fieldName,
							previousType,
							fieldType);
				}
			}
		});

		return columnToType;
	}

	public static AdhocJooqTableWrapper newInstance(Map<String, ?> options) {
		AdhocJooqTableWrapperParametersBuilder parametersBuilder = AdhocJooqTableWrapperParameters.builder();

		String tableName;
		if (options.containsKey("tableName")) {
			tableName = MapPathGet.getRequiredString(options, "tableName");
			parametersBuilder.tableName(tableName);
		} else {
			tableName = "someTableName";
		}

		AdhocJooqTableWrapperParameters parameters = parametersBuilder.build();
		return new AdhocJooqTableWrapper(tableName, parameters);
	}

	public DSLContext makeDsl() {
		return dbParameters.getDslSupplier().getDSLContext();
	}

	@Override
	public IAggregatedRecordStream streamSlices(TableQuery tableQuery) {
		IAdhocJooqTableQueryFactory queryFactory = makeQueryFactory();

		ResultQuery<Record> resultQuery = queryFactory.prepareQuery(tableQuery);

		if (tableQuery.isExplain() || tableQuery.isDebug()) {
			log.info("[EXPLAIN] SQL to db={}: `{}`", getName(), resultQuery.getSQL(ParamType.INLINED));
			// resultQuery.fields()
		}
		if (tableQuery.isDebug()) {
			debugResultQuery(resultQuery);
		}

		AggregatedRecordFields fields = queryFactory.makeSelectedColumns(tableQuery);

		Stream<IAggregatedRecord> tableStream = toMapStream(fields, resultQuery);

		return new SuppliedAggregatedRecordStream(tableQuery, () -> tableStream);
	}

	protected IAdhocJooqTableQueryFactory makeQueryFactory() {
		DSLContext dslContext = makeDsl();

		IAdhocJooqTableQueryFactory queryFactory = makeQueryFactory(dslContext);
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

	protected IAdhocJooqTableQueryFactory makeQueryFactory(DSLContext dslContext) {
		return AdhocJooqTableQueryFactory.builder().table(dbParameters.getTable()).dslContext(dslContext).build();
	}

	protected Stream<IAggregatedRecord> toMapStream(AggregatedRecordFields fields, ResultQuery<Record> sqlQuery) {
		return sqlQuery.stream().map(r -> intoMap(fields, r));

	}

	// Inspired from AbstractRecord.intoMap
	// Take original `queriedColumns` as the record may not clearly expresses aliases (e.g. `p.name` vs `name`). And it
	// is ambiguous to build a `columnName` from a `Name`.
	protected IAggregatedRecord intoMap(AggregatedRecordFields fields, Record r) {
		Map<String, Object> aggregates = new LinkedHashMap<>();

		List<String> aggregateFields = fields.getAggregates();
		{
			int size = aggregateFields.size();
			for (int i = 0; i < size; i++) {
				String columnName = aggregateFields.get(i);

				Object previousValue = aggregates.put(columnName, r.get(i));
				if (previousValue != null) {
					throw new InvalidResultException("Field " + columnName + " is not unique in Record : " + r);
				}
			}
		}

		Map<String, Object> groupBys = new LinkedHashMap<>();
		{
			List<String> groupByFields = fields.getColumns();
			int size = groupByFields.size();
			for (int i = 0; i < size; i++) {
				String columnName = groupByFields.get(i);

				Object previousValue = groupBys.put(columnName, r.get(aggregateFields.size() + i));
				if (previousValue != null) {
					throw new InvalidResultException("Field " + columnName + " is not unique in Record : " + r);
				}
			}
		}

		return AggregatedRecordOverMaps.builder().aggregates(aggregates).groupBys(groupBys).build();
	}

	protected String toQualifiedName(Field<?> field) {
		// field.getQualifiedName()
		return Stream.of(field.getQualifiedName().parts()).map(Name::first).collect(Collectors.joining("."));
	}

}
