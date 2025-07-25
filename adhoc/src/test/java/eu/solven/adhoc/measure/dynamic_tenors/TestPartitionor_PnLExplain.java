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
package eu.solven.adhoc.measure.dynamic_tenors;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.beta.schema.RelevancyHeuristic;
import eu.solven.adhoc.beta.schema.RelevancyHeuristic.CubeRelevancy;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.combination.EvaluatedExpressionCombination;
import eu.solven.adhoc.measure.decomposition.DuplicatingDecomposition;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.sum.CoalesceAggregation;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;

/**
 * Consider a model where Adhoc received {@link Map}, providing some aggregable values (e.g. some risk/sensitivity (e.g.
 * delta)) along some tenor (time left before expiry) and maturity (total lifespan, i.e. tenor at inception).
 * 
 * Tenor and Maturity are provided by input {@link Map}. Then ,they are not provided as plain columns. IColumnGenerator
 * will play a role.
 * 
 * BEWARE THIS IS A VERY BAD DESIGN AS IT LEADS TO MASSIVE DUPLICATION OF DATA, DUE TO THE CARTESIAN PRODUCTS BETWEEN
 * THE DUPLICATED COLUMNS.
 * 
 * @author Benoit Lacelle
 */
// https://www.investopedia.com/terms/t/tenor.asp#toc-tenor-vs-maturity
public class TestPartitionor_PnLExplain extends ADagTest implements IExamplePnLExplainConstant {

	@BeforeEach
	@Override
	public void feedTable() {
		table().add(Map.of("color",
				"blue",
				"sensitivities",
				MarketRiskSensitivity.empty().addDelta(Map.of(K_TENOR, "1Y", K_MATURITY, "2Y"), 12.34D)));
		table().add(Map.of("color",
				"red",
				"sensitivities",
				MarketRiskSensitivity.empty().addDelta(Map.of(K_TENOR, "1Y", K_MATURITY, "2Y"), 23.45D)));

		table().add(Map.of("color",
				"blue",
				"sensitivities",
				MarketRiskSensitivity.empty().addDelta(Map.of(K_TENOR, "3M", K_MATURITY, "1Y"), 34.56D)));
		table().add(Map.of("color",
				"red",
				"sensitivities",
				MarketRiskSensitivity.empty().addDelta(Map.of(K_TENOR, "3M", K_MATURITY, "2Y"), 45.67D)));
	}

	@BeforeEach
	public void registerMeasures() {
		forest.addMeasure(Aggregator.builder()
				.name("sensitivities.notPropagated")
				.columnName("sensitivities")
				.aggregationKey(MarketRiskSensitivityAggregation.class.getName())
				.build());

		forest.addMeasure(Dispatchor.builder()
				.name("sensitivities")
				.underlying("sensitivities.notPropagated")
				.decompositionKey(DuplicatingDecomposition.class.getName())
				.decompositionOption("columnToCoordinates", Map.of(K_TENOR, TENORS, K_MATURITY, TENORS))
				.aggregationKey(CoalesceAggregation.KEY)
				.build());

		forest.addMeasure(Combinator.builder()
				.name("delta")
				.underlying("sensitivities")
				.combinationKey(ReduceSensitivitiesCombination.class.getName())
				.build());

		// Enable a `count(*)`, which counts `1` per sensitivity
		// An alternative could be to keep track of count in `MarketRiskSensitivity` objects. But it would lead to
		// computing counts even if most queries may not need them. This would fix the issue of `grandTotal` count `1`
		// for the 2 sensitivity with same tenor+maturity.
		forest.addMeasure(Combinator.builder()
				.name("count")
				.underlying("sensitivities")
				.combinationKey(ReduceSensitivitiesCombination.class.getName())
				.combinationOption("count", true)
				.build());

		forest.addMeasure(Combinator.builder()
				.name("market_shift")
				.combinationKey(MarketDataShiftCombination.class.getName())
				// Evaluate MarketData anywhere we have a sensitivity
				.underlying("delta")
				.build());

		forest.addMeasure(Partitionor.builder()
				.name("pnl_explain")
				.groupBy(GroupByColumns.named(K_TENOR, K_MATURITY))
				.combinationKey(EvaluatedExpressionCombination.KEY)
				.combinationOptions(Map.of(EvaluatedExpressionCombination.K_EXPRESSION,
						"if (underlyings[0] == null, null, underlyings[0] * underlyings[1])"))
				.underlyings(Arrays.asList("delta", "market_shift"))
				.build());
	}

	@Test
	public void testQueryGrandTotal() {
		ITabularView output = cube().execute(CubeQuery.builder().measure("pnl_explain", "count").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(),
						Map.of("pnl_explain",
								(12.34D + 23.45) * 2.7 + 34.56D * 4.6 + 45.67D * 7.7,
								"count",
								// 3 instead of 4 as the pre-aggregation did not count
								0L + 3))
				.hasSize(1);
	}

	@Test
	public void testGroupByTenor() {
		ITabularView output = cube().execute(CubeQuery.builder().measure("pnl_explain").groupByAlso(K_TENOR).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(K_TENOR, "1Y"),
						Map.of("pnl_explain",
								BigDecimal.valueOf(12.34D + 23.45).multiply(BigDecimal.valueOf(2.7)).doubleValue()))
				.containsEntry(Map.of(K_TENOR, "3M"),
						Map.of("pnl_explain",
								BigDecimal.valueOf(34.56).multiply(BigDecimal.valueOf(4.6)).doubleValue()
										+ BigDecimal.valueOf(45.67).multiply(BigDecimal.valueOf(7.7)).doubleValue()))
				.hasSize(2);
	}

	@Test
	public void testGroupByTenor_FilterBlue() {
		ITabularView output = cube().execute(CubeQuery.builder()
				.measure("pnl_explain", "count")
				.groupByAlso(K_TENOR)
				.andFilter("color", "blue")
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(K_TENOR, "1Y"),
						Map.of("pnl_explain",
								BigDecimal.valueOf(12.34D).multiply(BigDecimal.valueOf(2.7)).doubleValue(),
								"count",
								1L))
				.containsEntry(Map.of(K_TENOR, "3M"),
						Map.of("pnl_explain",
								BigDecimal.valueOf(34.56).multiply(BigDecimal.valueOf(4.6)).doubleValue(),
								"count",
								1L))
				.hasSize(2);
	}

	@Test
	public void testGroupByTenor_filterMaturity() {
		ITabularView output = cube().execute(CubeQuery.builder()
				.measure("pnl_explain")
				.groupByAlso(K_TENOR)
				.andFilter(ColumnFilter.isLike(K_MATURITY, "2Y"))
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(K_TENOR, "1Y"),
						Map.of("pnl_explain",
								BigDecimal.valueOf(12.34D + 23.45).multiply(BigDecimal.valueOf(2.7)).doubleValue()))
				.containsEntry(Map.of(K_TENOR, "3M"),
						Map.of("pnl_explain",
								BigDecimal.valueOf(45.67).multiply(BigDecimal.valueOf(7.7)).doubleValue()))
				.hasSize(2);
	}

	@Test
	public void testGrandTotal_filterMaturity() {
		ITabularView output = cube().execute(
				CubeQuery.builder().measure("pnl_explain").andFilter(ColumnFilter.isLike(K_MATURITY, "2Y")).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(),
						Map.of("pnl_explain",
								BigDecimal.valueOf(12.34D + 23.45).multiply(BigDecimal.valueOf(2.7)).doubleValue()
										+ BigDecimal.valueOf(45.67).multiply(BigDecimal.valueOf(7.7)).doubleValue()))
				.hasSize(1);
	}

	@Test
	public void testRelevancyHeuristics() {
		CubeRelevancy relevancy = new RelevancyHeuristic().computeRelevancies(forest);

		Assertions.assertThat(relevancy.getColumnToRelevancy()).hasSize(2).hasEntrySatisfying("maturity", r -> {
			Assertions.assertThat(r.getScore()).isCloseTo(1.0, Offset.offset(0.001));
		}).hasEntrySatisfying("tenor", r -> {
			Assertions.assertThat(r.getScore()).isCloseTo(1.0, Offset.offset(0.001));
		});

		Assertions.assertThat(relevancy.getMeasureToRelevancy()).hasSize(3).hasEntrySatisfying("market_shift", r -> {
			Assertions.assertThat(r.getScore()).isCloseTo(4.0, Offset.offset(0.001));
		}).hasEntrySatisfying("pnl_explain", r -> {
			Assertions.assertThat(r.getScore()).isCloseTo(4.0, Offset.offset(0.001));
		}).hasEntrySatisfying("sensitivities.notPropagated", r -> {
			Assertions.assertThat(r.getScore()).isCloseTo(1.0, Offset.offset(0.001));
		});
	}
}
