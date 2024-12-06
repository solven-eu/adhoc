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
package eu.solven.adhoc.database;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

import eu.solven.adhoc.query.DatabaseQuery;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Wraps a {@link Connection} and rely on JooQ to use it as database for {@link DatabaseQuery}.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Builder
@Slf4j
public class AdhocJooqSqlDatabaseWrapper implements IAdhocDatabaseWrapper {
	@Builder.Default
	@NonNull
	@Getter
	final IAdhocDatabaseTranscoder transcoder = new IdentityTranscoder();

	@NonNull
	final Supplier<Connection> connectionSupplier;

	@NonNull
	final String tableName;

	public DSLContext makeDsl() {
		return DSL.using(connectionSupplier.get());
	}

	protected Map<String, ?> transcodeFromDb(IAdhocDatabaseReverseTranscoder transcodingContext,
			Map<String, ?> underlyingMap) {
		return AdhocTranscodingHelper.transcode(transcodingContext, underlyingMap);
	}

	@Override
	public Stream<Map<String, ?>> openDbStream(DatabaseQuery dbQuery) {
		TranscodingContext transcodingContext = TranscodingContext.builder().transcoder(transcoder).build();

		DSLContext dslContext = makeDsl();

		AdhocJooqSqlDatabaseStreamOpener streamOpener = makeTranscodedStreamOpener(transcodingContext, dslContext);

		ResultQuery<Record> resultQuery = streamOpener.prepareQuery(dbQuery);

		if (dbQuery.isExplain() || dbQuery.isDebug()) {
			log.info("[EXPLAIN] SQL to db: `{}`", resultQuery.getSQL(ParamType.INLINED));
		}
		if (dbQuery.isDebug()) {
			// "column_name",
			// "column_type",
			// "null",
			// "key",
			// "default",
			// "extra"
			Map<Object, Object> columnNameToType =
					makeDsl().fetchStream("DESCRIBE FROM %s".formatted(DSL.name(tableName)))
							.collect(Collectors.toMap(r -> r.get("column_name"), r -> r.get("column_type")));

			log.info("[DEBUG] {}", columnNameToType);
		}

		Stream<Map<String, ?>> dbStream = resultQuery.stream().map(Record::intoMap);

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

	private AdhocJooqSqlDatabaseStreamOpener makeTranscodedStreamOpener(TranscodingContext transcodingContext,
			DSLContext dslContext) {
		return AdhocJooqSqlDatabaseStreamOpener.builder()
				.transcoder(transcodingContext)
				.tableName(tableName)
				.dslContext(dslContext)
				.build();
	}

}
