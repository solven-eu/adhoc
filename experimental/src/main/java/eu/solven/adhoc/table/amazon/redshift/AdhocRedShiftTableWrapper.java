/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p>
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

import lombok.Builder;

import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.TabularRecordOverMaps;
import eu.solven.adhoc.table.sql.IJooqTableQueryFactory;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.util.NotYetImplementedException;
import lombok.extern.slf4j.Slf4j;
import org.jooq.conf.ParamType;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataAsyncClient;
import software.amazon.awssdk.services.redshiftdata.model.*;

/**
 * Enables querying Google BigQuery.
 *
 * @author Benoit Lacelle
 */
// https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html
@Slf4j
public class AdhocRedShiftTableWrapper extends JooqTableWrapper {

    final AdhocRedshiftTableWrapperParameters redShiftParameters;

    @Builder
    public AdhocRedShiftTableWrapper(String name, AdhocRedshiftTableWrapperParameters redShiftParameters) {
        super(name, redShiftParameters.getBase());

        this.redShiftParameters = redShiftParameters;
    }

    @Override
    protected Stream<ITabularRecord> toMapStream(IJooqTableQueryFactory.QueryWithLeftover sqlQuery) {
        String sql = sqlQuery.getQuery().getSQL(ParamType.INLINED);

        String sqlStatement = "SELECT * FROM Movies WHERE year = :year";
        SqlParameter yearParam = SqlParameter.builder()
                .name("year")
                .value(String.valueOf(1324))
                .build();

        ExecuteStatementRequest statementRequest = ExecuteStatementRequest.builder()
//				.clusterIdentifier(clusterId)
                .database("dev")
                .dbUser("admin")
                .parameters(yearParam)
                .sql(sqlStatement)
                .build();

        try {
            CompletableFuture.supplyAsync(() -> {
                try {
                    ExecuteStatementResponse response = getAsyncDataClient().executeStatement(statementRequest).join(); // Use join() to wait for the result
                    return response.id();
                } catch (RedshiftDataException e) {
                    throw new RuntimeException("Error executing statement: " + e.getMessage(), e);
                }
            }).exceptionally(exception -> {
                log.info("Error: {}", exception.getMessage());
                return "ERROR-%s".formatted(exception.getMessage());
            }).thenApply(statementId -> {
                GetStatementResultRequest resultRequest = GetStatementResultRequest.builder()
                        .id(statementId)
                        .build();

                return getAsyncDataClient().getStatementResult(resultRequest)
                        .handle((response, exception) -> {
                            if (exception != null) {
                                log.info("Error getting statement result {} ", exception.getMessage());
                                throw new RuntimeException("Error getting statement result: " + exception.getMessage(), exception);
                            }

                            List<ColumnMetadata> metadata = response.columnMetadata();

                            // Extract and print the field values using streams if the response is valid.
                            response.records().stream().map(row -> {
                                Map<String, Object> aggregates = new LinkedHashMap<>();

                                {
                                    List<String> aggregateColumns = sqlQuery.getFields().getAggregates();

                                    for (int i = 0; i < aggregateColumns.size(); i++) {
                                        Field field = row.get(i);

                                        Object value;
                                        if (Field.Type.LONG_VALUE.equals(field.type())) {
                                            value = field.longValue();
                                        } else  if (Field.Type.DOUBLE_VALUE.equals(field.type())) {
                                            value = field.doubleValue();
                                        } else {
                                            throw new NotYetImplementedException(String.valueOf(field.type()));
                                        }

                                        String columnName = aggregateColumns.get(i);
                                        aggregates.put(columnName, value);
                                    }
                                }

                                Map<String, Object> slice = new LinkedHashMap<>();

                                {
                                    List<String> aggregateGroupBys = sqlQuery.getFields().getColumns();
                                    for (int i = 0; i < aggregateGroupBys.size(); i++) {
                                        Field field = schema.getFields().get(aggregateGroupBys.size() + i);

                                        Object value;
                                        FieldValue fieldValue = row.get(aggregateGroupBys.size() + i);
                                        if (LegacySQLTypeName.INTEGER.equals(field.getType())) {
                                            value = fieldValue.getLongValue();
                                        } else {
                                            value = fieldValue.getValue();
                                        }

                                        String columnName = aggregateGroupBys.get(i);
                                        aggregates.put(columnName, value);
                                    }
                                }

                                if (!sqlQuery.getFields().getLateColumns().isEmpty()) {
                                    throw new NotYetImplementedException("TODO");
                                }

                                return TabularRecordOverMaps.builder().aggregates(aggregates).slice(slice).build();
                            });

                            return response;
                        }).join();
            }).thenApply(result -> {
                // Process the result here
                log.info("Result: {}", result);
                return result;
            });
        } catch (RuntimeException rt) {
            Throwable cause = rt.getCause();
            if (cause instanceof RedshiftDataException redshiftEx) {
                log.info("Redshift Data error occurred: {} Error code: {}", redshiftEx.getMessage(), redshiftEx.awsErrorDetails().errorCode());
            } else {
                log.info("An unexpected error occurred: {}", rt.getMessage());
            }
            throw cause;
        }
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
