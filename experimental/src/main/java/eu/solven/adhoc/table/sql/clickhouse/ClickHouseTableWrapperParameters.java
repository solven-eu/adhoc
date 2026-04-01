/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.table.sql.clickhouse;

import com.clickhouse.client.api.Client;

import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;

/**
 * Helps configuring a {@link ClickHouseTableWrapper}.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
// https://clickhouse.com/docs/integrations/language-clients/java/jdbc
public class ClickHouseTableWrapperParameters {
	@NonNull
	JooqTableWrapperParameters base;

	private static final int DEFAULT_MIN_SPLIT_ROWS = 1024;

	/**
	 * Minimum rows in a loaded batch before {@link eu.solven.adhoc.table.arrow.ArrowFixedBatchSpliterator} will split
	 * it. Splitting below this threshold produces tasks too small to amortise FJP coordination overhead.
	 */
	@Default
	int minSplitRows = DEFAULT_MIN_SPLIT_ROWS;

	// 2 futures in flight: one being processed by FJP workers, one loading on a VT.
	private static final int DEFAULT_PREFETCH_COUNT = 2;

	/**
	 * Maximum number of Arrow batch-loading futures kept in flight during parallel streaming. Each future runs on a
	 * virtual thread from {@code adhocMixedPool}; they are chained so the Arrow reader is never accessed concurrently.
	 * Only active in parallel mode ({@code stream.parallel()}); sequential streaming is unaffected.
	 */
	@Default
	int prefetchCount = DEFAULT_PREFETCH_COUNT;

	@Default
	Client client = new Client.Builder().addEndpoint("http://localhost:8123")
			.setUsername("default")
			.setPassword("")
			.setMaxConnections(AdhocUnsafe.getParallelism())
			.build();
}
