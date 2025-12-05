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
package eu.solven.adhoc.table.amazon.redshift;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.jooq.conf.ParamType;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.TabularRecordOverMaps;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.map.StandardSliceFactory.MapBuilderPreKeys;
import eu.solven.adhoc.table.sql.IJooqTableQueryFactory;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.util.NotYetImplementedException;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataAsyncClient;
import software.amazon.awssdk.services.redshiftdata.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.redshiftdata.model.ExecuteStatementResponse;
import software.amazon.awssdk.services.redshiftdata.model.Field;
import software.amazon.awssdk.services.redshiftdata.model.GetStatementResultRequest;
import software.amazon.awssdk.services.redshiftdata.model.RedshiftDataException;

/**
 * Enables querying Google BigQuery.
 *
 * @author Benoit Lacelle
 */
// https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html
@Slf4j
public class AdhocRedshiftTableWrapper extends JooqTableWrapper {

	final AdhocRedshiftTableWrapperParameters redShiftParameters;

	@Builder(builderMethodName = "redshift")
	public AdhocRedshiftTableWrapper(String name, AdhocRedshiftTableWrapperParameters redShiftParameters) {
		super(name, redShiftParameters.getBase());

		this.redShiftParameters = redShiftParameters;
	}

	@Override
	protected Stream<ITabularRecord> toMapStream(QueryPod queryPod, IJooqTableQueryFactory.QueryWithLeftover sqlQuery) {
		// TODO Would it be relevant to pop `NAMED` SQL and rely on `SqlParameter`?
		String sqlStatement = sqlQuery.getQuery().getSQL(ParamType.INLINED);

		// String sqlStatement = "SELECT * FROM Movies WHERE year = :year";
		// SqlParameter yearParam = SqlParameter.builder().name("year").value(String.valueOf(1324)).build();

		ExecuteStatementRequest statementRequest = ExecuteStatementRequest.builder()
				// .clusterIdentifier(clusterId)
				.database(redShiftParameters.getDatabase())
				.dbUser(redShiftParameters.getDbUser())
				.workgroupName(redShiftParameters.getWorkgroupName())
				// .parameters(yearParam)
				.sql(sqlStatement)
				.build();

		// try {
		CompletableFuture<Stream<ITabularRecord>> completable = CompletableFuture.supplyAsync(() -> {
			try {
				ExecuteStatementResponse response = getAsyncDataClient().executeStatement(statementRequest).join();
				return response.id();
			} catch (RedshiftDataException e) {
				throw new RuntimeException("Error executing statement: " + e.getMessage(), e);
			}
		}).exceptionally(exception -> {
			log.info("Error: {}", exception.getMessage());
			return "ERROR-%s".formatted(exception.getMessage());
		}).thenApply(statementId -> {
			GetStatementResultRequest resultRequest = GetStatementResultRequest.builder().id(statementId).build();

			return getAsyncDataClient().getStatementResult(resultRequest)
					.<Stream<ITabularRecord>>handle((response, exception) -> {
						if (exception != null) {
							log.info("Error getting statement result {} ", exception.getMessage());
							throw new RuntimeException("Error getting statement result: " + exception.getMessage(),
									exception);
						}

						// Extract and print the field values using streams if the response is valid.
						return response.records().stream().map(row -> toTabularRecord(sqlQuery, row));
					})
					.join();
		}).thenApply(result -> {
			// Process the result here
			log.info("Result: {}", result);
			return result;
		});

		return completable.join();
		// } catch (RuntimeException rt) {
		// Throwable cause = rt.getCause();
		// if (cause instanceof RedshiftDataException redshiftEx) {
		// log.info("Redshift Data error occurred: {} Error code: {}",
		// redshiftEx.getMessage(),
		// redshiftEx.awsErrorDetails().errorCode());
		// } else {
		// log.info("An unexpected error occurred: {}", rt.getMessage());
		// }
		// throw rt;
		// }
	}

	protected TabularRecordOverMaps toTabularRecord(IJooqTableQueryFactory.QueryWithLeftover sqlQuery,
			List<Field> row) {
		Map<String, Object> aggregates = new LinkedHashMap<>();

		{
			List<String> aggregateColumns = sqlQuery.getFields().getAggregates();

			for (int i = 0; i < aggregateColumns.size(); i++) {
				Field field = row.get(i);

				Object value = toObject(field);

				String columnName = aggregateColumns.get(i);
				aggregates.put(columnName, value);
			}
		}

		@NonNull
		ImmutableList<String> aggregateGroupBys = sqlQuery.getFields().getColumns();
		MapBuilderPreKeys slice = sliceFactory.newMapBuilder(aggregateGroupBys);

		{
			for (int i = 0; i < aggregateGroupBys.size(); i++) {
				Field field = row.get(aggregateGroupBys.size() + i);

				Object value = toObject(field);
				slice.append(value);
			}
		}

		if (!sqlQuery.getFields().getLeftovers().isEmpty()) {
			throw new NotYetImplementedException("leftovers=%s".formatted(sqlQuery.getFields().getLeftovers()));
		}

		return TabularRecordOverMaps.builder().aggregates(aggregates).slice(slice.build().asSlice()).build();
	}

	protected Object toObject(Field field) {
		return switch (field.type()) {
		case Field.Type.LONG_VALUE:
			yield field.longValue();
		case Field.Type.DOUBLE_VALUE:
			yield field.doubleValue();
		case Field.Type.IS_NULL:
			yield null;
		case Field.Type.STRING_VALUE:
			yield field.stringValue();
		case Field.Type.BOOLEAN_VALUE:
			yield field.booleanValue();
		case Field.Type.BLOB_VALUE:
			yield field.blobValue();
		default:
			throw new NotYetImplementedException(String.valueOf(field.type()));
		};
	}

	protected RedshiftDataAsyncClient getAsyncDataClient() {
		return redShiftParameters.getAsyncDataClient();
	}

	@Override
	protected void debugResultQuery(IJooqTableQueryFactory.QueryWithLeftover resultQuery) {
		// Default behavior is not valid as we do not have a JDBC Connection to execute the DEBUG SQL
		log.info("[DEBUG] TODO Amazon Redshift");
	}
}
