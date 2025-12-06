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
package eu.solven.adhoc.engine;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.table.InMemoryTable;
import eu.solven.adhoc.util.AdhocUnsafe;
import nl.jqno.equalsverifier.EqualsVerifier;

public class TestQueryPod {
	@Test
	public void testHashcodeEquals() {
		EqualsVerifier.forClass(QueryPod.class).verify();
	}

	@Test
	public void testForTable() {
		InMemoryTable table = InMemoryTable.builder().build();
		QueryPod queryContext = QueryPod.forTable(table);

		Assertions.assertThat(queryContext.getTable()).isSameAs(table);
	}

	@Test
	public void testColumnManager() {
		InMemoryTable table = InMemoryTable.builder().build();
		IColumnsManager columnManager = Mockito.mock(IColumnsManager.class);

		QueryPod queryContext = QueryPod.builder()
				.table(table)
				.forest(MeasureForest.empty())
				.query(CubeQuery.builder().build())
				.columnsManager(columnManager)
				.build();

		Assertions.assertThat(queryContext.getColumnsManager()).isSameAs(columnManager);
	}

	@Test
	public void testDefaultExecutorService() {
		InMemoryTable table = InMemoryTable.builder().build();
		QueryPod queryContext = QueryPod.forTable(table);

		Assertions.assertThat(queryContext.getExecutorService().getClass().getName())
				.isEqualTo("com.google.common.util.concurrent.DirectExecutorService");
	}

	@Test
	public void testCustomExecutorService() {
		InMemoryTable table = InMemoryTable.builder().build();
		QueryPod queryContext =
				QueryPod.forTable(table).toBuilder().executorService(AdhocUnsafe.adhocCommonPool).build();

		Assertions.assertThat(queryContext.getExecutorService().toString())
				.startsWith("com.google.common.util.concurrent.MoreExecutors$ListeningDecorator")
				.contains("java.util.concurrent.ForkJoinPool");

		Assertions.assertThat(queryContext.getExecutorService().getClass().getName())
				// java.util.concurrent.ForkJoinPool
				.isEqualTo("com.google.common.util.concurrent.MoreExecutors$ListeningDecorator");
	}
}
