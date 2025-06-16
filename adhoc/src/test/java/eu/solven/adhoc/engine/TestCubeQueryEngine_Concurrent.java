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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Phaser;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ARawDagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.composite.PhasedTableWrapper;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestCubeQueryEngine_Concurrent extends ARawDagTest implements IAdhocTestConstants {
	// This can be increased to check given tests are valid even if run many times
	// It is especially important as concurrency is prone to race-conditions, leading to bugs happening not
	// deterministically.
	int nbLoops = 4;

	@Override
	public ITableWrapper makeTable() {
		return PhasedTableWrapper.builder().name("phased").build();
	}

	@Test
	public void testConcurrentTableQueries() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);
		forest.addMeasure(sum_MaxK1K2ByA);

		PhasedTableWrapper phasedTable = (PhasedTableWrapper) table();

		// We expect 2 queries: one for grandTotal, and one groupedBy:A
		phasedTable.getPhasers().bulkRegister(2);

		ITabularView view = cube().execute(
				CubeQuery.builder().measure(k1Sum, sum_MaxK1K2ByA).option(StandardQueryOptions.CONCURRENT).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(view);
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 1, sum_MaxK1K2ByA.getName(), 0L + 1))
				.hasSize(1);

		Phaser opening = phasedTable.getPhasers().getOpening();
		Assertions.assertThat(opening.getPhase()).isEqualTo(1);
		Assertions.assertThat(opening.getArrivedParties()).isEqualTo(0);

		Phaser streaming = phasedTable.getPhasers().getStreaming();
		Assertions.assertThat(streaming.getPhase()).isEqualTo(1);
		Assertions.assertThat(streaming.getArrivedParties()).isEqualTo(0);

		Phaser closing = phasedTable.getPhasers().getClosing();
		Assertions.assertThat(closing.getPhase()).isEqualTo(1);
		Assertions.assertThat(closing.getArrivedParties()).isEqualTo(0);
	}

	@Test
	public void testConcurrentTableQueries_differByFilter() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(filterK1onA1);

		PhasedTableWrapper phasedTable = (PhasedTableWrapper) table();

		// We expect 1 query: one for grandTotal, and one filtered on A1, both in same SQL given FILTER
		phasedTable.getPhasers().bulkRegister(1);

		ITabularView view = cube().execute(
				CubeQuery.builder().measure(k1Sum, filterK1onA1).option(StandardQueryOptions.CONCURRENT).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(view);
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 1, filterK1onA1.getName(), 0L + 1))
				.hasSize(1);

		Phaser opening = phasedTable.getPhasers().getOpening();
		Assertions.assertThat(opening.getPhase()).isEqualTo(1);
		Assertions.assertThat(opening.getArrivedParties()).isEqualTo(0);

		Phaser streaming = phasedTable.getPhasers().getStreaming();
		Assertions.assertThat(streaming.getPhase()).isEqualTo(1);
		Assertions.assertThat(streaming.getArrivedParties()).isEqualTo(0);

		Phaser closing = phasedTable.getPhasers().getClosing();
		Assertions.assertThat(closing.getPhase()).isEqualTo(1);
		Assertions.assertThat(closing.getArrivedParties()).isEqualTo(0);
	}

	@Slf4j
	public static class PhasedCombinator implements ICombination {

		final Phaser phaser;

		public PhasedCombinator(Map<String, ?> options) {
			phaser = MapPathGet.getRequiredAs(options, "phaser");
		}

		@Override
		public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
			log.info("opening arriveAndAwaitAdvance() {}", phaser);
			phaser.arriveAndAwaitAdvance();
			log.info("opening advance");

			return underlyingValues.getFirst();
		}
	}

	@Test
	public void testConcurrentQuerySteps() {
		PhasedTableWrapper phasedTable = (PhasedTableWrapper) table();

		int nbConcurrentSteps = 2;
		Phaser phaser = new Phaser(0);

		forest.addMeasure(k1Sum);

		for (int i = 0; i < nbConcurrentSteps; i++) {
			forest.addMeasure(Combinator.builder()
					.name(k1Sum.getName() + "_phased_" + i)
					.combinationKey(PhasedCombinator.class.getName())
					.combinationOptions(Map.of("phaser", phaser))
					.underlying(k1Sum.getName())
					.build());

		}
		forest.addMeasure(Combinator.builder()
				.name("sum_phased")
				.underlyings(
						IntStream.range(0, nbConcurrentSteps).mapToObj(i -> k1Sum.getName() + "_phased_" + i).toList())
				.build());

		// We expect 1 queries: k1Sum grandTotal
		int phaseTable = phasedTable.getPhasers().bulkRegister(1);
		int stepPhase = phaser.bulkRegister(nbConcurrentSteps);
		if (phaseTable != stepPhase) {
			throw new IllegalStateException("opening != closing (%s != %s)".formatted(phaseTable, stepPhase));
		} else {
			log.info("query phase={}", phaseTable);
		}

		int loopIndex = -1;
		while (loopIndex++ < nbLoops) {
			int phaseBeforeStart = phaser.getPhase();
			log.info("query phase={}", phaseBeforeStart);
			Assertions.assertThat(phaseBeforeStart).isEqualTo(loopIndex);

			ITabularView view = cube()
					.execute(CubeQuery.builder().measure("sum_phased").option(StandardQueryOptions.CONCURRENT).build());

			MapBasedTabularView mapBased = MapBasedTabularView.load(view);
			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of(), Map.of("sum_phased", 0L + 2))
					.hasSize(1);

			// Phaser opening = phasedTable.getPhasers().getOpening();
			// Assertions.assertThat(opening.getPhase()).isEqualTo(1);
			// Assertions.assertThat(opening.getArrivedParties()).isEqualTo(0);
			//
			// Phaser streaming = phasedTable.getPhasers().getStreaming();
			// Assertions.assertThat(streaming.getPhase()).isEqualTo(1);
			// Assertions.assertThat(streaming.getArrivedParties()).isEqualTo(0);
			//
			// Phaser closing = phasedTable.getPhasers().getClosing();
			// Assertions.assertThat(closing.getPhase()).isEqualTo(1);
			// Assertions.assertThat(closing.getArrivedParties()).isEqualTo(0);
		}
	}

}
