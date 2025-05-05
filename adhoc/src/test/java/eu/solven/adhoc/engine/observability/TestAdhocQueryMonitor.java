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
package eu.solven.adhoc.engine.observability;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.eventbus.Subscribe;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.eventbus.QueryLifecycleEvent;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.query.cube.CubeQuery;

public class TestAdhocQueryMonitor extends ADagTest {
	public static class AdhocQueryMonitorGuava extends AdhocQueryMonitor {
		public AdhocQueryMonitorGuava(int maxSlowestQueries) {
			super(maxSlowestQueries);
		}

		@Override
		@Subscribe
		public void onQueryLifecycleEvent(QueryLifecycleEvent lifecycleEvent) {
			super.onQueryLifecycleEvent(lifecycleEvent);
		}
	}

	AtomicReference<OffsetDateTime> now = new AtomicReference<>(OffsetDateTime.now());

	AdhocQueryMonitorGuava queryMonitor = new AdhocQueryMonitorGuava(1) {
		protected OffsetDateTime now() {
			return now.get();
		};
	};

	@BeforeEach
	public void registerMonitor() {
		eventBus.register(queryMonitor);
	}

	@BeforeEach
	@Override
	public void feedTable() {
		table.add(Map.of("color", "blue"));
	}

	public static class LatchingCombination implements ICombination {
		Runnable runnable;

		public LatchingCombination(Map<String, ?> options) {
			runnable = (Runnable) options.get("latch");
		}

		@Override
		public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
			runnable.run();

			return null;
		}
	}

	@Test
	public void testGrandTotal() {
		AtomicBoolean hasCheckWhileRunning = new AtomicBoolean();
		Runnable r = () -> {
			hasCheckWhileRunning.set(true);
			Assertions.assertThat(queryMonitor.queryToStart).hasSize(1);
			Assertions.assertThat(queryMonitor.slowestQueried).isEmpty();
		};

		forest.addMeasure(Aggregator.countAsterisk());
		forest.addMeasure(Combinator.builder()
				.name("latch")
				.combinationKey(LatchingCombination.class.getName())
				.combinationOptions(Map.of("latch", r))
				.underlying(Aggregator.countAsterisk().getName())
				.build());

		Assertions.assertThat(hasCheckWhileRunning).isFalse();

		ITabularView view = cube.execute(CubeQuery.builder().measure("latch").build());
		Assertions.assertThat(view.isEmpty()).isTrue();

		Assertions.assertThat(hasCheckWhileRunning).isTrue();

		Assertions.assertThat(queryMonitor.queryToStart).isEmpty();
		Assertions.assertThat(queryMonitor.slowestQueried).hasSize(1);
	}

	@Test
	public void testFastThenSlow() {
		AtomicInteger queryIndex = new AtomicInteger();

		Runnable r = () -> {
			// Simulate time moving forward during evaluation
			now.set(now.get().plusSeconds(queryIndex.getAndIncrement()));
		};

		forest.addMeasure(Aggregator.countAsterisk());
		forest.addMeasure(Combinator.builder()
				.name("latch")
				.combinationKey(LatchingCombination.class.getName())
				.combinationOptions(Map.of("latch", r))
				.underlying(Aggregator.countAsterisk().getName())
				.build());

		Assertions.assertThat(queryMonitor.queryToStart).isEmpty();
		Assertions.assertThat(queryMonitor.slowestQueried).isEmpty();

		// fast
		ITabularView viewFast = cube.execute(CubeQuery.builder().measure("latch").build());
		Assertions.assertThat(viewFast.isEmpty()).isTrue();

		Assertions.assertThat(queryMonitor.queryToStart).isEmpty();
		Assertions.assertThat(queryMonitor.slowestQueried).hasSize(1);
		Assertions.assertThat(queryMonitor.slowestQueried.peek().getKey().getQuery().getGroupBy().getGroupedByColumns())
				.hasSize(0);

		// slow
		ITabularView viewSlow = cube.execute(CubeQuery.builder().measure("latch").groupByAlso("color").build());
		Assertions.assertThat(viewSlow.isEmpty()).isTrue();

		Assertions.assertThat(queryMonitor.queryToStart).isEmpty();
		Assertions.assertThat(queryMonitor.slowestQueried).hasSize(1);
		Assertions.assertThat(queryMonitor.slowestQueried.peek().getKey().getQuery().getGroupBy().getGroupedByColumns())
				.hasSize(1);
	}
}
