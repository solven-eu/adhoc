/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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

import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataAsyncClient;

@Value
@Builder
public class AdhocRedshiftTableWrapperParameters {
	@NonNull
	JooqTableWrapperParameters base;

	@NonNull
	RedshiftDataAsyncClient asyncDataClient;

	//
	// @NonNull
	// @Default
	// final BigQueryOptions bigQueryOptions = BigQueryOptions.getDefaultInstance();
	//
	// // TODO Is there scenarios where we do not rely on the same projectId as from BigQueryOptions?
	// @NonNull
	// @Default
	// final String projectId = BigQueryOptions.getDefaultInstance().getProjectId();
	//
	// /**
	// * BEWARE This will not define underlying default dialect to MYSQL.
	// *
	// * @return
	// */
	// public static AdhocBigQueryTableWrapperParametersBuilder builder() {
	// return new AdhocBigQueryTableWrapperParametersBuilder();
	// }
	//
	// public static AdhocBigQueryTableWrapperParametersBuilder builder(Name tableName) {
	// return new AdhocBigQueryTableWrapperParametersBuilder().base(JooqTableWrapperParameters.builder()
	// // Google BigQuery seems not very far from MySQL
	// .dslSupplier(() -> DSL.using(SQLDialect.MYSQL))
	// .tableName(tableName)
	// .build());
	// }
}
