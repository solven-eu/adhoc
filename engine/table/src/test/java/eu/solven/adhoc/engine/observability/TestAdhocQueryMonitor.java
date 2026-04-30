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
package eu.solven.adhoc.engine.observability;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.eventbus.QueryLifecycleEvent;
import eu.solven.adhoc.table.InMemoryTable;
import eu.solven.adhoc.table.StandaloneTableQueryPod;

public class TestAdhocQueryMonitor {

	private QueryLifecycleEvent startEvent(StandaloneTableQueryPod pod) {
		return QueryLifecycleEvent.builder()
				.query(pod)
				.tag(AdhocQueryMonitor.TAG_QUERY_LIFECYCLE)
				.tag(AdhocQueryMonitor.TAG_QUERY_START)
				.build();
	}

	private QueryLifecycleEvent doneEvent(StandaloneTableQueryPod pod) {
		return QueryLifecycleEvent.builder()
				.query(pod)
				.tag(AdhocQueryMonitor.TAG_QUERY_LIFECYCLE)
				.tag(AdhocQueryMonitor.TAG_QUERY_DONE)
				.build();
	}

	private StandaloneTableQueryPod pod() {
		return StandaloneTableQueryPod.builder().table(InMemoryTable.builder().build()).build();
	}

	@Test
	public void testStart_recordsActiveQuery() {
		AdhocQueryMonitor monitor = new AdhocQueryMonitor();
		StandaloneTableQueryPod pod = pod();

		monitor.onQueryLifecycleEvent(startEvent(pod));

		Assertions.assertThat(monitor.queryToStart).hasSize(1).containsKey(pod);
		Assertions.assertThat(monitor.slowestQueried).isEmpty();
	}

	@Test
	public void testStart_then_done_movesToSlowest() {
		// A complete lifecycle: start → done. The pod leaves the active set and lands in the slow-queries queue
		// with its measured duration.
		AdhocQueryMonitor monitor = new AdhocQueryMonitor();
		StandaloneTableQueryPod pod = pod();

		monitor.onQueryLifecycleEvent(startEvent(pod));
		monitor.onQueryLifecycleEvent(doneEvent(pod));

		Assertions.assertThat(monitor.queryToStart).isEmpty();
		Assertions.assertThat(monitor.slowestQueried).hasSize(1);
	}

	@Test
	public void testDone_withoutStart_doesNotCrash() {
		// Lone done event (lost start, or duplicate done): logged as a warning but must not blow up the monitor.
		AdhocQueryMonitor monitor = new AdhocQueryMonitor();
		StandaloneTableQueryPod pod = pod();

		Assertions.assertThatCode(() -> monitor.onQueryLifecycleEvent(doneEvent(pod))).doesNotThrowAnyException();
		Assertions.assertThat(monitor.queryToStart).isEmpty();
		Assertions.assertThat(monitor.slowestQueried).isEmpty();
	}

	@Test
	public void testStart_twiceOnSamePod_warnsButReplaces() {
		// Duplicate start on the same pod: the helper logs a warning but the second start replaces the first
		// (so duration timing remains coherent vs the latest start). queryToStart still contains the pod once.
		AdhocQueryMonitor monitor = new AdhocQueryMonitor();
		StandaloneTableQueryPod pod = pod();

		monitor.onQueryLifecycleEvent(startEvent(pod));

		Assertions.assertThatCode(() -> monitor.onQueryLifecycleEvent(startEvent(pod))).doesNotThrowAnyException();
		Assertions.assertThat(monitor.queryToStart).hasSize(1);
	}

	@Test
	public void testEvent_neitherStartNorDone_isIgnored() {
		// Event tagged with only QUERY_LIFECYCLE — no specific start/done semantic. Monitor must remain inert.
		AdhocQueryMonitor monitor = new AdhocQueryMonitor();
		StandaloneTableQueryPod pod = pod();

		QueryLifecycleEvent unrelated =
				QueryLifecycleEvent.builder().query(pod).tag(AdhocQueryMonitor.TAG_QUERY_LIFECYCLE).build();

		monitor.onQueryLifecycleEvent(unrelated);

		Assertions.assertThat(monitor.queryToStart).isEmpty();
		Assertions.assertThat(monitor.slowestQueried).isEmpty();
	}

	@Test
	public void testCustomMaxSlowQueries_evictsBeyondCapacity() {
		// Capacity 2 → after 3 completed queries, only 2 entries remain in slowestQueried (the slowest are kept;
		// the fastest is evicted because the priority queue is ordered by ascending duration).
		AdhocQueryMonitor monitor = new AdhocQueryMonitor(2);

		for (int i = 0; i < 3; i++) {
			StandaloneTableQueryPod pod = pod();
			monitor.onQueryLifecycleEvent(startEvent(pod));
			monitor.onQueryLifecycleEvent(doneEvent(pod));
		}

		Assertions.assertThat(monitor.queryToStart).isEmpty();
		Assertions.assertThat(monitor.slowestQueried).hasSize(2);
	}

	@Test
	public void testMultipleConcurrentQueries_independentlyTracked() {
		AdhocQueryMonitor monitor = new AdhocQueryMonitor();
		StandaloneTableQueryPod podA = pod();
		StandaloneTableQueryPod podB = pod();

		monitor.onQueryLifecycleEvent(startEvent(podA));
		monitor.onQueryLifecycleEvent(startEvent(podB));

		Assertions.assertThat(monitor.queryToStart).hasSize(2).containsKeys(podA, podB);

		monitor.onQueryLifecycleEvent(doneEvent(podA));

		Assertions.assertThat(monitor.queryToStart).hasSize(1).containsKey(podB);
		Assertions.assertThat(monitor.slowestQueried).hasSize(1);
	}

	@Test
	public void testDefaultConstructor_usesDefaultMax() {
		// Default constructor wires the standard 100-slot slow-queries queue.
		AdhocQueryMonitor monitor = new AdhocQueryMonitor();

		Assertions.assertThat(monitor.slowestQueriedMax).isEqualTo(100);
	}
}
