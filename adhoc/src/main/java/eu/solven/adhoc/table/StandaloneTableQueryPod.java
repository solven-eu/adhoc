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
package eu.solven.adhoc.table;

import java.time.OffsetDateTime;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.map.factory.RowSliceFactory;
import eu.solven.adhoc.options.IQueryOption;
import eu.solven.adhoc.query.AdhocQueryId;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;

/**
 * A thin standalone {@link ITableQueryPod} suitable for tests, edge-cases and metadata calls (column sampling,
 * cardinality estimation) that need to drive an {@link ITableWrapper#streamSlices} without a full cube/engine
 * {@code QueryPod}. Defaults: empty options, direct executor, {@link RowSliceFactory}, never cancelled, queryId derived
 * from {@code table.getName()}.
 *
 * @author Benoit Lacelle
 * @see ITableQueryPod#forTable(ITableWrapper)
 */
@Builder
@Getter
public class StandaloneTableQueryPod implements ITableQueryPod {

	@NonNull
	ITableWrapper table;

	@Default
	AdhocQueryId queryId = null;

	@NonNull
	@Default
	ImmutableSet<IQueryOption> options = ImmutableSet.of();

	@NonNull
	@Default
	ListeningExecutorService executorService = MoreExecutors.newDirectExecutorService();

	@NonNull
	@Default
	ISliceFactory sliceFactory = RowSliceFactory.builder().build();

	public AdhocQueryId getQueryId() {
		// Lazy default: derived from the table name so logs/cache keys remain meaningful even without a cube context.
		if (queryId == null) {
			return AdhocQueryId.builder().cube(table.getName()).cubeElseTable(false).build();
		}
		return queryId;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public OffsetDateTime getCancellationDate() {
		return null;
	}

	@Override
	public void addCancellationListener(Runnable runnable) {
		// no-op: a standalone pod has no cancellation channel
	}

	@Override
	public void removeCancellationListener(Runnable runnable) {
		// no-op
	}

	@Override
	public ITableQueryPod withTable(ITableWrapper newTable) {
		return StandaloneTableQueryPod.builder()
				.table(newTable)
				.queryId(queryId)
				.options(options)
				.executorService(executorService)
				.sliceFactory(sliceFactory)
				.build();
	}
}
