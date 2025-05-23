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
package eu.solven.adhoc.table.google.bigquery;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.jooq.conf.ParamType;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;

import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.TabularRecordOverMaps;
import eu.solven.adhoc.table.sql.IJooqTableQueryFactory;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.util.NotYetImplementedException;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * Enables querying Google BigQuery.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class AdhocBigQueryTableWrapper extends JooqTableWrapper {

	final AdhocBigQueryTableWrapperParameters bigQueryParameters;

	@Builder(builderMethodName = "bigquery")
	public AdhocBigQueryTableWrapper(String name, AdhocBigQueryTableWrapperParameters bigQueryParameters) {
		super(name, bigQueryParameters.getBase());

		this.bigQueryParameters = bigQueryParameters;
	}

	@Override
	protected Stream<ITabularRecord> toMapStream(IJooqTableQueryFactory.QueryWithLeftover sqlQuery) {
		String sql = sqlQuery.getQuery().getSQL(ParamType.INLINED);

		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql)
				// Use standard SQL syntax for queries.
				// See: https://cloud.google.com/bigquery/sql-reference/
				.setUseLegacySql(false)
				.build();

		BigQuery bigquery = bigQueryParameters.getBigQueryOptions().getService();

		JobId jobId = JobId.newBuilder().setProject(bigQueryParameters.getBigQueryOptions().getProjectId()).build();
		Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

		// Wait for the query to complete.
		try {
			queryJob = queryJob.waitFor();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}

		// Check for errors
		if (queryJob == null) {
			throw new RuntimeException("Job no longer exists");
		} else if (queryJob.getStatus().getError() != null) {
			// You can also look at queryJob.getStatus().getExecutionErrors() for all
			// errors, not just the latest one.
			throw new RuntimeException(queryJob.getStatus().getError().toString());
		}

		// Get the results.
		TableResult result;
		try {
			result = queryJob.getQueryResults();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		} catch (JobException e) {
			throw new IllegalStateException(e);
		}

		Schema schema = result.getSchema();

		// Print all pages of the results.
		return result.streamAll().map(row -> {
			Map<String, Object> aggregates = new LinkedHashMap<>();

			{
				List<String> aggregateColumns = sqlQuery.getFields().getAggregates();
				for (int i = 0; i < aggregateColumns.size(); i++) {
					Field field = schema.getFields().get(i);

					Object value;
					FieldValue fieldValue = row.get(i);
					if (LegacySQLTypeName.INTEGER.equals(field.getType())) {
						value = fieldValue.getLongValue();
					} else {
						value = fieldValue.getValue();
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
	}

	@Override
	protected void debugResultQuery(IJooqTableQueryFactory.QueryWithLeftover resultQuery) {
		// Default behavior is not valid as we do not have a JDBC Connection to execute the DEBUG SQL
		log.info("[DEBUG] TODO Google BigQuery");
	}
}
