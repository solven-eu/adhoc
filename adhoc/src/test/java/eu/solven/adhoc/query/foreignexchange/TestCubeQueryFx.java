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
package eu.solven.adhoc.query.foreignexchange;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.measure.ratio.AdhocExplainerTestHelper;
import eu.solven.adhoc.measure.sum.SumElseSetAggregation;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import lombok.NonNull;

/**
 * This tests a typical financial use-case: given financial data for different currencies, we need to convert the
 * underlying data into a common currency, by a multiplication with the proper rate, depending on current currency. Each
 * data are aggregated by their own currency, then converted into a common currency, and finally aggregated.
 */
public class TestCubeQueryFx extends ADagTest implements IAdhocTestConstants {

	ForeignExchangeStorage fxStorage = new ForeignExchangeStorage();

	LocalDate today = LocalDate.now();

	public final CubeQueryEngine engine =
			editEngine().factories(makeFactories().toBuilder().operatorFactory(makeOperatorsFactory(fxStorage)).build())
					.build();
	public final CubeWrapper cube = editCube().engine(engine).build();

	private @NonNull IOperatorFactory makeOperatorsFactory(IForeignExchangeStorage fxStorage) {

		return new StandardOperatorFactory() {
			@Override
			public ICombination makeCombination(String key, Map<String, ?> options) {
				return switch (key) {
				case ForeignExchangeCombination.KEY: {
					yield new ForeignExchangeCombination(fxStorage);
				}
				default:
					yield super.makeCombination(key, options);
				};
			}
		};
	}

	@Override
	@BeforeEach
	public void feedTable() {
		table.add(Map.of("color", "red", "letter", "a", "ccyFrom", "USD", "k1", 123));
		table.add(Map.of("color", "red", "letter", "b", "ccyFrom", "EUR", "k1", 234));
	}

	String mName = "k1.CCY";

	void prepareMeasures() {
		forest.addMeasure(Partitionor.builder()
				.name(mName)
				.underlyings(Arrays.asList(k1Sum.getName()))
				.groupBy(GroupByColumns.named("ccyFrom"))
				.combinationKey(ForeignExchangeCombination.KEY)
				// Error codes (e.g. lack of FX) are aggregated in a Set
				.aggregationKey(SumElseSetAggregation.class.getName())
				.build());

		forest.addMeasure(k1Sum);
	}

	@Test
	public void testNoFx() {
		prepareMeasures();

		ITabularView output = cube.execute(CubeQuery.builder().measure(mName).build());

		// List<Map<String, ?>> keySet =
		// output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		// Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of(mName, Set.of("Missing_FX_Rate-%s-USD-EUR".formatted(today))));
	}

	@Test
	public void testHasUnknownFromCcy() {
		prepareMeasures();

		table.add(Map.of("color", "red", "ccyFrom", "unknownCcy", "k1", 234));

		ITabularView output = cube.execute(CubeQuery.builder().measure(mName).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of(mName,
								Set.of("Missing_FX_Rate-%s-unknownCcy-EUR".formatted(today),
										"Missing_FX_Rate-%s-USD-EUR".formatted(today))));
	}

	@Test
	public void testHasUnknownToCcy() {
		prepareMeasures();

		// Need a bit more than 1 USD for 1 EUR
		fxStorage.addFx(IForeignExchangeStorage.FXKey.builder().fromCcy("USD").toCcy("EUR").build(), 0.95D);

		ITabularView output = cube.execute(CubeQuery.builder().measure(mName).customMarker(Optional.of("XYZ")).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of(mName,
								Set.of("Missing_FX_Rate-%s-USD-XYZ".formatted(today),
										"Missing_FX_Rate-%s-EUR-XYZ".formatted(today))));
	}

	@Test
	public void testGrandTotalInJpy_customMarker() {
		prepareMeasures();

		// Need many JPY for one USD
		fxStorage.addFx(IForeignExchangeStorage.FXKey.builder().fromCcy("USD").toCcy("JPY").build(), 150D);
		// Need a bit more than 1 USD for 1 EUR
		fxStorage.addFx(IForeignExchangeStorage.FXKey.builder().fromCcy("USD").toCcy("EUR").build(), 0.95D);

		ITabularView output =
				cube.execute(CubeQuery.builder().measure(mName).customMarker(Optional.of("JPY")).explain(true).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(mName, 0D + 123 * 150D + 234 / 0.95D * 150D));
	}

	@Test
	public void testLackBucketor() {
		// We miss a partitionor aggregating the converted value per ccyFrom
		{
			forest.addMeasure(Partitionor.builder()
					.name(mName)
					.underlyings(Arrays.asList("k1"))
					.combinationKey(ForeignExchangeCombination.KEY)
					.build());

			forest.addMeasure(k1Sum);
		}

		Assertions.setMaxStackTraceElementsDisplayed(128);

		Assertions.assertThatThrownBy(() -> cube.execute(CubeQuery.builder().measure(mName).build()))
				.isInstanceOf(IllegalStateException.class)
				.hasRootCauseInstanceOf(IllegalArgumentException.class)
				.hasStackTraceContaining("ccyFrom is not a sliced column");
	}

	@Test
	public void testExplain_grandTotal() {
		List<String> messages = AdhocExplainerTestHelper.listenForExplainNoPerf(eventBus);

		prepareMeasures();

		// ITabularView output =
		cube.execute(CubeQuery.builder().measure(mName).customMarker(Optional.of("JPY")).explain(true).build());

		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n")))
				.isEqualTo(
						"""
								#0 s=inMemory id=00000000-0000-0000-0000-000000000000
								\\-- #1 m=k1.CCY(Partitionor[FX][eu.solven.adhoc.measure.sum.SumElseSetAggregation]) filter=matchAll groupBy=grandTotal customMarker=JPY
								    \\-- #2 m=k1(SUM) filter=matchAll groupBy=(ccyFrom) customMarker=JPY""");

		Assertions.assertThat(messages).hasSize(3);
	}

	@Test
	public void testExplain_filter() {
		List<String> messages = AdhocExplainerTestHelper.listenForExplainNoPerf(eventBus);

		prepareMeasures();

		// ITabularView output =
		cube.execute(CubeQuery.builder()
				.measure(mName)
				.customMarker(Optional.of("JPY"))
				.groupByAlso("letter")
				.andFilter("color", "red")
				.explain(true)
				.build());

		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n")))
				.isEqualTo(
						"""
								#0 s=inMemory id=00000000-0000-0000-0000-000000000000
								\\-- #1 m=k1.CCY(Partitionor[FX][eu.solven.adhoc.measure.sum.SumElseSetAggregation]) filter=color==red groupBy=(letter) customMarker=JPY
								    \\-- #2 m=k1(SUM) filter=color==red groupBy=(ccyFrom, letter) customMarker=JPY""");

		Assertions.assertThat(messages).hasSize(3);
	}

	@Test
	public void testLogPerfs() {
		List<String> messages = AdhocExplainerTestHelper.listenForPerf(eventBus);

		prepareMeasures();

		// ITabularView output =
		cube.execute(CubeQuery.builder()
				.measure(mName)
				.customMarker(Optional.of("JPY"))
				.groupByAlso("letter")
				.andFilter("color", "red")
				.explain(true)
				.build());

		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n")))
				.isEqualToNormalizingNewlines(
						"""
								time=2ms for openingStream on TableQueryV2(filter=color==red, groupBy=(ccyFrom, letter), aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN])
								time=4ms for mergeTableAggregates on TableQueryV2(filter=color==red, groupBy=(ccyFrom, letter), aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN])
								time=5ms sizes=[2] for toSortedColumns on TableQueryV2(filter=color==red, groupBy=(ccyFrom, letter), aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN])
								#0 s=inMemory id=00000000-0000-0000-0000-000000000000
								|  No cost info
								\\-- #1 m=k1.CCY(Partitionor[FX][eu.solven.adhoc.measure.sum.SumElseSetAggregation]) filter=color==red groupBy=(letter) customMarker=JPY
								    |  size=2 duration=6ms
								    \\-- #2 m=k1(SUM) filter=color==red groupBy=(ccyFrom, letter) customMarker=JPY
								        \\  size=2 duration=12ms
								Executed status=OK duration=21ms on table=inMemory forest=TestCubeQueryFx-filtered query=CubeQuery(filter=color==red, groupBy=(letter), measures=[ReferencedMeasure(ref=k1.CCY)], customMarker=JPY, options=[EXPLAIN])""");

		Assertions.assertThat(messages).hasSize(7);
	}

}
