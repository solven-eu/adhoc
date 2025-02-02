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
package eu.solven.adhoc.database.sql;

import java.sql.Connection;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.ResultQuery;
import org.jooq.SelectJoinStep;
import org.jooq.conf.ParamType;
import org.jooq.exception.InvalidResultException;
import org.jooq.impl.DSL;

import eu.solven.adhoc.database.IAdhocTableWrapper;
import eu.solven.adhoc.database.IRowsStream;
import eu.solven.adhoc.database.SuppliedRowsStream;
import eu.solven.adhoc.database.sql.AdhocJooqTableWrapperParameters.AdhocJooqTableWrapperParametersBuilder;
import eu.solven.adhoc.database.transcoder.AdhocTranscodingHelper;
import eu.solven.adhoc.database.transcoder.IAdhocTableReverseTranscoder;
import eu.solven.adhoc.database.transcoder.TranscodingContext;
import eu.solven.adhoc.query.TableQuery;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Wraps a {@link Connection} and rely on JooQ to use it as database for {@link TableQuery}.
 *
 * @author Benoit Lacelle
 */
@AllArgsConstructor
@Slf4j
public class AdhocJooqTableWrapper implements IAdhocTableWrapper {

	final AdhocJooqTableWrapperParameters dbParameters;

	@Override
	public String getName() {
		return "someTableName";
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
		return Stream.of(select).collect(Collectors.<Field, String, Class<?>>toMap(f -> f.getName(), f -> f.getType()));
	}

	public static AdhocJooqTableWrapper newInstance(Map<String, ?> options) {
		AdhocJooqTableWrapperParametersBuilder parametersBuilder = AdhocJooqTableWrapperParameters.builder();

		if (options.containsKey("tableName")) {
			parametersBuilder.tableName(MapPathGet.getRequiredString(options, "tableName"));
		}

		AdhocJooqTableWrapperParameters parameters = parametersBuilder.build();
		return new AdhocJooqTableWrapper(parameters);
	}

	public DSLContext makeDsl() {
		return dbParameters.getDslSupplier().getDSLContext();
	}

	protected Map<String, ?> transcodeFromDb(IAdhocTableReverseTranscoder transcodingContext,
			Map<String, ?> underlyingMap) {
		return AdhocTranscodingHelper.transcode(transcodingContext, underlyingMap);
	}

	@Override
	public IRowsStream openDbStream(TableQuery dbQuery) {
		TranscodingContext transcodingContext = openTranscodingContext();

		IAdhocJooqTableQueryFactory queryFactory = makeQueryFactory(transcodingContext);

		ResultQuery<Record> resultQuery = queryFactory.prepareQuery(dbQuery);

		if (dbQuery.isExplain() || dbQuery.isDebug()) {
			log.info("[EXPLAIN] SQL to db: `{}`", resultQuery.getSQL(ParamType.INLINED));
		}
		if (dbQuery.isDebug()) {
			debugResultQuery(resultQuery);
		}

		Stream<Map<String, ?>> dbStream = toMapStream(resultQuery);

		return new SuppliedRowsStream(() -> cleanStream(dbQuery, dbStream, transcodingContext));
	}

	private Stream<Map<String, ?>> cleanStream(TableQuery dbQuery,
			Stream<Map<String, ?>> dbStream,
			TranscodingContext transcodingContext) {
		return dbStream.filter(row -> {
			// We could have a fallback, to filter manually when it is not doable by the DB (or we do not know how to
			// build the proper filter)
			return true;
		}).<Map<String, ?>>map(notTranscoded -> {
			Map<String, Object> aggregatorValues = new LinkedHashMap<>();
			dbQuery.getAggregators().forEach(a -> {
				String aggregatorName = a.getName();
				Object aggregatedValue = notTranscoded.remove(aggregatorName);
				if (aggregatedValue == null) {
					// SQL groupBy returns `a=null` even if there is not a single matching row
					notTranscoded.remove(aggregatorName);
				} else {
					aggregatorValues.put(aggregatorName, aggregatedValue);
				}
			});

			if (aggregatorValues.isEmpty()) {
				// There is not a single non-null aggregate: discard the whole Map (including groupedBy columns)
				return Map.of();
			} else {
				// In case of manual filters, we may have to hide some some columns, needed by the manual filter, but
				// unexpected by the output stream

				// We transcode only groupBy columns, as an aggregator may have a name matching an underlying column
				Map<String, ?> transcoded = transcodeFromDb(transcodingContext, notTranscoded);

				// ImmutableMap does not accept null value. How should we handle missing value i ngroupBy, when returned
				// as null by DB?
				// return ImmutableMap.<String, Object>builderWithExpectedSize(transcoded.size() +
				// aggregatorValues.size())
				// .putAll(transcoded)
				// .putAll(aggregatorValues)
				// .build();

				Map<String, Object> merged = new LinkedHashMap<>();

				merged.putAll(transcoded);
				merged.putAll(aggregatorValues);
				return merged;
			}
		})
				// Filter-out the groups which does not have a single aggregatedValue
				.filter(m -> !m.isEmpty());
	}

	private IAdhocJooqTableQueryFactory makeQueryFactory(TranscodingContext transcodingContext) {
		DSLContext dslContext = makeDsl();

		IAdhocJooqTableQueryFactory queryFactory = makeQueryFactory(transcodingContext, dslContext);
		return queryFactory;
	}

	private TranscodingContext openTranscodingContext() {
		return TranscodingContext.builder().transcoder(dbParameters.getTranscoder()).build();
	}

	protected void debugResultQuery(ResultQuery<Record> resultQuery) {
		DSLContext dslContext = makeDsl();

		// This would fail in case of complex `from` expression, like one with JOINs
		SelectJoinStep<Record1<Object>> query =
				dslContext.select(DSL.field(DSL.unquotedName("DESCRIBE"))).from(dbParameters.getTable());
		try {
			// "column_name",
			// "column_type",
			// "null",
			// "key",
			// "default",
			// "extra"

			Map<Object, Object> columnNameToType = dslContext.fetchStream(query)
					.collect(Collectors.toMap(r -> r.get("column_name"), r -> r.get("column_type")));

			log.info("[DEBUG] {}", columnNameToType);

			// https://stackoverflow.com/questions/76908078/how-to-check-if-a-query-is-valid-or-not-using-jooq
			String plan = dslContext.explain(resultQuery).plan();

			log.info("[DEBUG] Query plan: {}", plan);

		} catch (RuntimeException e) {
			log.debug("[DEBUG] Issue executing debug-query: {}", query, e);
		}
	}

	protected IAdhocJooqTableQueryFactory makeQueryFactory(TranscodingContext transcodingContext,
			DSLContext dslContext) {
		return AdhocJooqTableQueryFactory.builder()
				.transcoder(transcodingContext)
				.table(dbParameters.getTable())
				.dslContext(dslContext)
				.build();
	}

	protected Stream<Map<String, ?>> toMapStream(ResultQuery<Record> sqlQuery) {
		return sqlQuery.stream().map(this::intoMap);

	}

	// Inspired from AbstractRecord.intoMap
	protected Map<String, Object> intoMap(Record r) {
		Map<String, Object> map = new LinkedHashMap<>();

		List<Field<?>> fields = Arrays.asList(r.fields());
		int size = fields.size();
		for (int i = 0; i < size; i++) {
			Field<?> field = fields.get(i);

			if (map.put(toQualifiedName(field), r.get(i)) != null) {
				throw new InvalidResultException("Field " + field.getName() + " is not unique in Record : " + this);
			}
		}

		return map;
	}

	protected String toQualifiedName(Field<?> field) {
		return Stream.of(field.getQualifiedName().parts()).map(Name::first).collect(Collectors.joining("."));
	}

}
