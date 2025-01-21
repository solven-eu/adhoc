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
package eu.solven.adhoc.google.bigquery;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.jooq.Record;
import org.jooq.ResultQuery;
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

import eu.solven.adhoc.database.sql.AdhocJooqDatabaseWrapper;
import lombok.extern.slf4j.Slf4j;

/**
 * Enables querying Google BigQuery.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class AdhocGoogleBigQueryDatabaseWrapper extends AdhocJooqDatabaseWrapper {

	final AdhocBigQueryDatabaseWrapperParameters dbParameters;

	public AdhocGoogleBigQueryDatabaseWrapper(AdhocBigQueryDatabaseWrapperParameters dbParameters) {
		super(dbParameters.getBase());

		this.dbParameters = dbParameters;
	}

	@Override
	protected Stream<Map<String, ?>> toMapStream(ResultQuery<Record> sqlQuery) {
		String sql = sqlQuery.getSQL(ParamType.INLINED);

		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql)
				// Use standard SQL syntax for queries.
				// See: https://cloud.google.com/bigquery/sql-reference/
				.setUseLegacySql(false)
				.build();

		BigQuery bigquery = dbParameters.getBigQueryOptions().getService();

		JobId jobId = JobId.newBuilder().setProject(dbParameters.getBigQueryOptions().getProjectId()).build();
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
			Map<String, Object> asMap = new LinkedHashMap<>();

			for (int i = 0; i < schema.getFields().size(); i++) {
				Field field = schema.getFields().get(i);

				Object value;
				FieldValue fieldValue = row.get(i);
				if (LegacySQLTypeName.INTEGER.equals(field.getType())) {
					value = fieldValue.getLongValue();
				} else {
					value = fieldValue.getValue();
				}
				asMap.put(field.getName(), value);
			}

			return asMap;
		});
	}

	// TODO The SuperBuilder class is more complex
	// public static class AdhocGoogleBigQueryDatabaseWrapperSuperBuilder {
	// /**
	// * This will reset the projectId to the bigQueryOptions projectId.
	// *
	// * @param bigQueryOptions
	// * @return the builder
	// */
	// // We should default the projectId to the bigQueryOptions projectId
	// // public AdhocGoogleBigQueryDatabaseWrapperBuilder setProjectId(BigQueryOptions bigQueryOptions) {
	// // setProjectId(bigQueryOptions.getProjectId());
	// //
	// // return this;
	// // }
	// }

	@Override
	protected void debugResultQuery() {
		// Default behavior is not valid as we do not have a JDBC Connection to execute the DEBUG SQL
		log.info("[DEBUG] TODO Google BigQuery");
	}
}
