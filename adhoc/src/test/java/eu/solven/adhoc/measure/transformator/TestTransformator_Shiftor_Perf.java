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
package eu.solven.adhoc.measure.transformator;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Shiftor;
import eu.solven.adhoc.measure.ratio.AdhocExplainerTestHelper;
import eu.solven.adhoc.measure.sum.SubstractionCombination;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.table.InMemoryTable;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.IStopwatchFactory;
import eu.solven.adhoc.util.NotYetImplementedException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestTransformator_Shiftor_Perf extends ADagTest implements IAdhocTestConstants {
	static final int maxCardinality = 100;
	static final int nbDays = 10_000;

	@BeforeAll
	public static void setLimits() {
		log.info("{} is evaluated on cardinality={}", TestTransformator_Shiftor_Perf.class.getName(), maxCardinality);
		AdhocUnsafe.limitColumnSize = maxCardinality * nbDays + 10;
	}

	@AfterAll
	public static void resetLimits() {
		AdhocUnsafe.resetProperties();
	}

	@Override
	public InMemoryTable makeTable() {
		return InMemoryTable.builder().build();
	}

	LocalDate today = LocalDate.now();

	@BeforeEach
	@Override
	public void feedTable() {
		for (int i = 0; i < maxCardinality; i++) {
			for (int d = 0; d < nbDays; d++) {
				table().add(ImmutableMap.<String, Object>builder()
						.put("l", "A")
						.put("row_index", i)
						.put("d", today.minusDays(d))
						.put("k1", (i + (nbDays - d) * (nbDays - d)))
						.build());
			}
		}
	}

	public IStopwatchFactory makeStopwatchFactory() {
		return IStopwatchFactory.guavaStopwatchFactory();
	}

	public static class PreviousDayFilterEditor implements IFilterEditor {

		@Override
		public IAdhocFilter editFilter(IAdhocFilter filter) {
			IValueMatcher valueMatcher = FilterHelpers.getValueMatcher(filter, "d");

			if (IValueMatcher.MATCH_ALL.equals(valueMatcher) || IValueMatcher.MATCH_NONE.equals(valueMatcher)) {
				return filter;
			} else if (valueMatcher instanceof EqualsMatcher equalsMatcher) {
				LocalDate shiftedDate = shiftDate((LocalDate) equalsMatcher.getOperand());
				return SimpleFilterEditor.shift(filter, "d", shiftedDate);
			} else {
				throw new NotYetImplementedException("Not managed: filter=%s".formatted(valueMatcher));
			}
		}

		protected LocalDate shiftDate(LocalDate date) {
			return date.minusDays(1);
		}

	}

	String m = "previousYesterday";
	String dToD = "dayToDay";

	@BeforeEach
	public void registerMeasures() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(Shiftor.builder()
				.name(m)
				.underlying(k1Sum.getName())
				.editorKey(PreviousDayFilterEditor.class.getName())
				.build());

		forest.addMeasure(Combinator.builder()
				.name(dToD)
				.underlyings(List.of(k1Sum.getName(), m))
				.combinationKey(SubstractionCombination.KEY)
				.build());
	}

	@Test
	public void testGrandTotal() {
		List<String> messages = AdhocExplainerTestHelper.listenForExplainNoPerf(eventBus);

		ITabularView output = cube().execute(CubeQuery.builder().measure(dToD).explain(true).build());

		log.info("Performance report:{}{}", "\r\n", messages.stream().collect(Collectors.joining("\r\n")));
		Assertions.assertThat(MapBasedTabularView.load(output).getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of(dToD, 0L));

	}

	@Test
	public void testGroupByDate_maxRow() {
		List<String> messages = AdhocExplainerTestHelper.listenForPerf(eventBus);

		ITabularView output = cube().execute(CubeQuery.builder()
				.measure(dToD)
				.groupByAlso("d")
				.andFilter("row_index", maxCardinality - 1)
				.explain(true)
				.build());

		Assertions.assertThat(MapBasedTabularView.load(output).getCoordinatesToValues())
				.hasSize(nbDays)
				.containsEntry(Map.of("d", today),
						Map.of(dToD,
								0L + (maxCardinality - 1 + (nbDays - 0) * (nbDays - 0))
										- (maxCardinality - 1 + (nbDays - 1) * (nbDays - 1))))
				.containsEntry(Map.of("d", today.minusDays(1)),
						Map.of(dToD,
								0L + (maxCardinality - 1 + (nbDays - 1) * (nbDays - 1))
										- (maxCardinality - 1 + (nbDays - 2) * (nbDays - 2))))
				.containsEntry(Map.of("d", today.minusDays(nbDays - 1)),
						Map.of(dToD, 0L + (maxCardinality - 1 + (nbDays - (nbDays - 1)) * (nbDays - (nbDays - 1)))
						// lastDay has no previous day
								- 0L));

		log.info("Performance report:{}{}", "\r\n", messages.stream().collect(Collectors.joining("\r\n")));
	}

	@Test
	public void testGroupByRow_Today() {
		List<String> messages = AdhocExplainerTestHelper.listenForPerf(eventBus);

		ITabularView output = cube().execute(
				CubeQuery.builder().measure(dToD).groupByAlso("row_index").andFilter("d", today).explain(true).build());

		Assertions.assertThat(MapBasedTabularView.load(output).getCoordinatesToValues())
				.hasSize(maxCardinality)
				.containsEntry(Map.of("row_index", 0),
						Map.of(dToD,
								0L + (maxCardinality + (nbDays) * (nbDays))
										- (maxCardinality + (nbDays - 1) * (nbDays - 1))))
				.containsEntry(Map.of("row_index", 1),
						Map.of(dToD,
								0L + (maxCardinality - 1 + (nbDays) * (nbDays))
										- (maxCardinality - 1 + (nbDays - 1) * (nbDays - 1))));

		log.info("Performance report:{}{}", "\r\n", messages.stream().collect(Collectors.joining("\r\n")));
	}

}
