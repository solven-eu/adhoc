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
package eu.solven.adhoc.engine.tabular;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;

import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.TableQueryOptimizer;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestTableQueryEngineBootstrapped_Concurrent {
	AdhocFactories factories = AdhocFactories.builder().build();

	@Test
	public void testConcurrentTableQueries() throws InterruptedException {
		TableQueryEngineBootstrapped engine = TableQueryEngineBootstrapped.builder()
				.optimizer(new TableQueryOptimizer(factories, factories.getFilterOptimizerFactory().makeOptimizer()))
				.build();

		ITableWrapper tableWrapper = Mockito.mock(ITableWrapper.class);
		Mockito.when(tableWrapper.getName()).thenReturn("someTableName");

		QueryPod queryPod =
				QueryPod.forTable(tableWrapper, CubeQuery.builder().option(StandardQueryOptions.CONCURRENT).build());

		CountDownLatch cdl = new CountDownLatch(2);

		Mockito.doAnswer(invok -> {
			Object query = invok.getArgument(1);
			log.info("Streaming query={}", query);
			cdl.countDown();

			// Await for the other queries to start at the same time
			cdl.await();

			return ITabularRecordStream.empty();
		}).when(tableWrapper).streamSlices(Mockito.eq(queryPod), Mockito.any(TableQueryV2.class));

		Future<?> future = AdhocUnsafe.adhocCommonPool.submit(() -> {

			Map<CubeQueryStep, ISliceToValue> views =
					engine.executeTableQueries(queryPod, (queryStep, SizeAndDuration) -> {
					},
							ImmutableSet.of(TableQueryV2.builder()
									.aggregator(FilteredAggregator.builder().aggregator(Aggregator.sum("a")).build())
									.build(),
									TableQueryV2.builder()
											.aggregator(FilteredAggregator.builder()
													.aggregator(Aggregator.sum("b"))
													.build())
											.build()));

			try {
				if (!cdl.await(1, TimeUnit.SECONDS)) {
					throw new IllegalStateException("CDL did not pass");
				}
			} catch (InterruptedException e) {
				Assertions.fail(e);
			}

			return views;
		});

		Assertions.assertThat(Futures.getUnchecked(future)).isNotNull();
	}
}
