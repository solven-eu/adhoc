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
package eu.solven.adhoc.pivotable.query;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.util.concurrent.MoreExecutors;

import eu.solven.adhoc.beta.schema.IAdhocSchema;
import eu.solven.adhoc.beta.schema.TargetedCubeQuery;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.pivotable.query.PivotableAsynchronousQueriesManager.StateAndView;
import eu.solven.adhoc.query.cube.CubeQuery;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestPivotableAsynchronousQueriesManager {
	final PivotableAsynchronousQueriesManager manager = new PivotableAsynchronousQueriesManager();
	final ExecutorService es = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());

	@AfterEach
	public void closeES() {
		manager.destroy();
		es.shutdown();
	}

	@Test
	public void testUnknownQueryId() {
		UUID unknownQueryId = UUID.randomUUID();

		Assertions.assertThat(manager.getState(unknownQueryId)).isEqualTo(AsynchronousStatus.UNKNOWN);
		StateAndView stateAndView = manager.getStateAndView(unknownQueryId);
		Assertions.assertThat(stateAndView.getState()).isEqualTo(AsynchronousStatus.UNKNOWN);
		Assertions.assertThat(stateAndView.getOptView()).isEmpty();
	}

	@Test
	public void testQueryId_runningThenServed() {
		IAdhocSchema schema = Mockito.mock(IAdhocSchema.class);
		TargetedCubeQuery query = TargetedCubeQuery.builder()
				.endpointId(UUID.randomUUID())
				.cube("someCube")
				.query(CubeQuery.builder().build())
				.build();

		// a CDL so we check the state for RUNNING
		CountDownLatch waitForPoll = new CountDownLatch(1);

		ITabularView view = Mockito.mock(ITabularView.class);
		Mockito.when(schema.executeAsync(query.getCube(), query.getQuery())).thenAnswer(invok -> {
			return es.submit(() -> {
				log.debug("Wait for CDL");
				waitForPoll.await();
				log.debug("Waited for CDL");

				return view;
			});
		});

		UUID queryId = manager.executeAsync(schema, query);

		{
			Assertions.assertThat(manager.getState(queryId)).isEqualTo(AsynchronousStatus.RUNNING);
			StateAndView stateAndView = manager.getStateAndView(queryId);
			Assertions.assertThat(stateAndView.getState()).isEqualTo(AsynchronousStatus.RUNNING);
			Assertions.assertThat(stateAndView.getOptView()).isEmpty();
		}

		waitForPoll.countDown();

		Awaitility.await().untilAsserted(() -> {
			Assertions.assertThat(manager.getState(queryId)).isEqualTo(AsynchronousStatus.SERVED);
			StateAndView stateAndView = manager.getStateAndView(queryId);
			Assertions.assertThat(stateAndView.getState()).isEqualTo(AsynchronousStatus.SERVED);
			Assertions.assertThat(stateAndView.getOptView()).contains(view);
		});
	}

	@Test
	public void testQueryId_runningThenFailed() {
		IAdhocSchema schema = Mockito.mock(IAdhocSchema.class);
		TargetedCubeQuery query = TargetedCubeQuery.builder()
				.endpointId(UUID.randomUUID())
				.cube("someCube")
				.query(CubeQuery.builder().build())
				.build();

		// a CDL so we check the state for RUNNING
		CountDownLatch waitForPoll = new CountDownLatch(1);

		Mockito.when(schema.executeAsync(query.getCube(), query.getQuery())).thenAnswer(invok -> {
			return es.submit(() -> {
				log.debug("Wait for CDL");
				waitForPoll.await();
				log.debug("Waited for CDL");

				throw new RuntimeException("Simulated failure");
			});
		});

		UUID queryId = manager.executeAsync(schema, query);

		{
			Assertions.assertThat(manager.getState(queryId)).isEqualTo(AsynchronousStatus.RUNNING);
			StateAndView stateAndView = manager.getStateAndView(queryId);
			Assertions.assertThat(stateAndView.getState()).isEqualTo(AsynchronousStatus.RUNNING);
			Assertions.assertThat(stateAndView.getOptView()).isEmpty();
		}

		waitForPoll.countDown();

		Awaitility.await().untilAsserted(() -> {
			Assertions.assertThat(manager.getState(queryId)).isEqualTo(AsynchronousStatus.FAILED);
			StateAndView stateAndView = manager.getStateAndView(queryId);
			Assertions.assertThat(stateAndView.getState()).isEqualTo(AsynchronousStatus.FAILED);
			Assertions.assertThat(stateAndView.getOptView()).isEmpty();
		});
	}
}
