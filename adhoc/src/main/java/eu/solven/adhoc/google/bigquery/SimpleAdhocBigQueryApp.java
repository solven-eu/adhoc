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

import org.jooq.Record2;
import org.jooq.SQLDialect;
import org.jooq.SelectLimitPercentStep;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

import eu.solven.adhoc.database.sql.DSLSupplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleAdhocBigQueryApp {

	public static void main(String... args) throws Exception {
		String projectId = "adhoc-testoverpublicdatasets";
		simpleApp(projectId);
	}

	public static void simpleApp(String projectId) {
		BigQueryOptions bigQueryOptions = BigQueryOptions.newBuilder()
				.setProjectId(projectId)
				// .setCredentials(credentials)
				.build();

		AdhocGoogleBigQueryDatabaseWrapper bgDbWrapper = AdhocGoogleBigQueryDatabaseWrapper.builder()
				.bigQueryOptions(bigQueryOptions)
				.dslSupplier(DSLSupplier.fromDialect(SQLDialect.CUBRID))
				.tableName(DSL.name("bigquery-public-data.stackoverflow.posts_questions"))
				.build();

		// bgDbWrapper.openDbStream(DatabaseQuery.builder().aggregators(null))

		try {
			BigQuery bigquery = bigQueryOptions.getService();

			// Google BigQuery seems not very far from MySQL
			SelectLimitPercentStep<Record2<Object, Object>> dsl = DSL.using(SQLDialect.MYSQL)
					.select(DSL.field("CONCAT('https://stackoverflow.com/questions/', CAST(id as STRING))").as("url"),
							DSL.field("view_count"))
					.from(DSL.table("bigquery-public-data.stackoverflow.posts_questions"))
					.where(DSL.field("tags").like("%google-bigquery%"))
					.orderBy(DSL.field("view_count").desc())
					.limit(10);

			String sql = dsl.getSQL(ParamType.INLINED);
			log.info("{}", sql);

			QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql)
					// Use standard SQL syntax for queries.
					// See: https://cloud.google.com/bigquery/sql-reference/
					.setUseLegacySql(false)
					.build();
			//
			// QueryJobConfiguration queryConfig = QueryJobConfiguration
			// .newBuilder("SELECT CONCAT('https://stackoverflow.com/questions/', "
			// + "CAST(id as STRING)) as url, view_count "
			// + "FROM `bigquery-public-data.stackoverflow.posts_questions` "
			// + "WHERE tags like '%google-bigquery%' "
			// + "ORDER BY view_count DESC "
			// + "LIMIT 10")
			// // Use standard SQL syntax for queries.
			// // See: https://cloud.google.com/bigquery/sql-reference/
			// .setUseLegacySql(false)
			// .build();

			JobId jobId = JobId.newBuilder().setProject(projectId).build();
			Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

			// Wait for the query to complete.
			queryJob = queryJob.waitFor();

			// Check for errors
			if (queryJob == null) {
				throw new RuntimeException("Job no longer exists");
			} else if (queryJob.getStatus().getError() != null) {
				// You can also look at queryJob.getStatus().getExecutionErrors() for all
				// errors, not just the latest one.
				throw new RuntimeException(queryJob.getStatus().getError().toString());
			}

			// Get the results.
			TableResult result = queryJob.getQueryResults();

			// Print all pages of the results.
			for (FieldValueList row : result.iterateAll()) {
				// String type
				String url = row.get("url").getStringValue();
				String viewCount = row.get("view_count").getStringValue();
				System.out.printf("%s : %s views\n", url, viewCount);
			}
		} catch (BigQueryException | InterruptedException e) {
			throw new IllegalStateException(e);
		}
	}
}