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
package eu.solven.adhoc.database.gcp.bigquery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.http.HttpTransportOptions;

import eu.solven.adhoc.column.TableExpressionColumn;
import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.top.AdhocTopClause;
import eu.solven.adhoc.table.google.bigquery.AdhocBigQueryTableWrapper;
import eu.solven.adhoc.table.google.bigquery.AdhocBigQueryTableWrapperParameters;
import eu.solven.pepper.unittest.PepperTestHelper;
import lombok.extern.slf4j.Slf4j;

// How to setup authentication: see the README.MD
@Slf4j
@Disabled("Various timeouts due to unknown reasons. Let's retry later")
public class TestTableGoogleBigQuery {

	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	@BeforeAll
	public static void assumeCredentialsAreAvailable() {
		try {
			BigQueryOptions.getDefaultInstance().getProjectId();
		} catch (IllegalArgumentException e) {
			log.warn("Lacking GCP credentials", e);
			Assumptions.assumeTrue(false, e.getMessage());
		}
	}

	@BeforeAll
	public static void assumeInternetIdOk() {
		PepperTestHelper.assumeInternetIsAvailable();
	}

	// @Test
	// public void testTableDoesNotExists() {
	// Assertions.assertThatThrownBy(() -> jooqDb.openDbStream(qK1).toList()).isInstanceOf(DataAccessException.class);
	// }
	//
	// @Test
	// public void testEmptyDb() {
	// dsl.createTableIfNotExists(tableName).column("k1", SQLDataType.DOUBLE).execute();
	//
	// List<Map<String, ?>> dbStream = jooqDb.openDbStream(qK1).collect(Collectors.toList());
	//
	// Assertions.assertThat(dbStream).isEmpty();
	// }

	@Test
	public void testPublicDataset() {
		// TODO This assume you have local credentials to this project
		// How to CI over a Google public dataset, without leaking credentials?
		String projectId = "adhoc-testoverpublicdatasets";

		BigQueryOptions bigQueryOptions = BigQueryOptions.newBuilder()
				.setProjectId(projectId)
				.setTransportOptions(
						HttpTransportOptions.newBuilder().setConnectTimeout(300).setReadTimeout(300).build())
				.setRetrySettings(RetrySettings.newBuilder().setMaxAttempts(3).build())
				.build();

		AdhocBigQueryTableWrapperParameters dbParameters = AdhocBigQueryTableWrapperParameters
				.builder(DSL.name("bigquery-public-data.stackoverflow.posts_questions"))
				.bigQueryOptions(bigQueryOptions)
				.build();
		AdhocBigQueryTableWrapper bgDbWrapper =
				AdhocBigQueryTableWrapper.bigquery().name("BigQuery").bigQueryParameters(dbParameters).build();

		List<Map<String, ?>> rows = bgDbWrapper.streamSlices(TableQuery.builder()
				.aggregator(Aggregator.sum("view_count"))
				.groupBy(GroupByColumns.of(TableExpressionColumn.builder()
						.name("url")
						.sql("CONCAT('https://stackoverflow.com/questions/', CAST(id as STRING))")
						.build()))
				.topClause(AdhocTopClause.builder().column(ReferencedColumn.ref("view_count")).limit(10).build())
				.build()).toList();

		Assertions.assertThat(rows).hasSize(10);

		List<Long> viewCounts = rows.stream().map(row -> (long) row.get("view_count")).toList();

		List<Long> sortedViewCounts = new ArrayList<>(viewCounts);
		Collections.sort(sortedViewCounts, Comparator.reverseOrder());

		Assertions.assertThat(viewCounts).isEqualTo(sortedViewCounts);
	}
}
