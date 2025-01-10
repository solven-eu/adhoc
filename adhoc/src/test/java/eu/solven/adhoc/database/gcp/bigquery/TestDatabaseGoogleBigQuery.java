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
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.BigQueryOptions;

import eu.solven.adhoc.google.bigquery.AdhocGoogleBigQueryDatabaseWrapper;
import eu.solven.adhoc.query.AdhocTopClause;
import eu.solven.adhoc.query.DatabaseQuery;
import eu.solven.adhoc.query.groupby.CalculatedColumn;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.groupby.ReferencedColumn;
import eu.solven.adhoc.transformers.Aggregator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestDatabaseGoogleBigQuery {

	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
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

		BigQueryOptions bigQueryOptions = BigQueryOptions.newBuilder().setProjectId(projectId).build();

		AdhocGoogleBigQueryDatabaseWrapper bgDbWrapper = AdhocGoogleBigQueryDatabaseWrapper.builder()
				.bigQueryOptions(bigQueryOptions)
				.tableName("bigquery-public-data.stackoverflow.posts_questions")
				.build();

		List<Map<String, ?>> rows = bgDbWrapper.openDbStream(DatabaseQuery.builder()
				.aggregator(Aggregator.sum("view_count"))
				.groupBy(GroupByColumns.of(CalculatedColumn.builder()
						.column("url")
						.sql("CONCAT('https://stackoverflow.com/questions/', CAST(id as STRING))")
						.build()))
				.topClause(AdhocTopClause.builder().column(ReferencedColumn.ref("view_count")).limit(10).build())
				.debug(true)
				.build()).collect(Collectors.toList());

		Assertions.assertThat(rows).hasSize(10);

		List<Long> viewCounts = rows.stream().map(row -> (long) row.get("view_count")).toList();

		List<Long> sortedViewCounts = new ArrayList<>(viewCounts);
		Collections.sort(sortedViewCounts, Comparator.reverseOrder());

		Assertions.assertThat(viewCounts).isEqualTo(sortedViewCounts);
	}
}
