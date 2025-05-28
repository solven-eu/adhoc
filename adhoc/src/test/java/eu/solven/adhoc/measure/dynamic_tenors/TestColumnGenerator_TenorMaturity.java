package eu.solven.adhoc.measure.dynamic_tenors;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.combination.EvaluatedExpressionCombination;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Bucketor;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.groupby.GroupByColumns;

/**
 * Consider a model where Adhoc received {@link Map}, providing some aggregable values (e.g. some risk/sensitivity (e.g.
 * delta)) along some tenor (time left before expiry) and maturity (total lifespan, i.e. tenor at inception).
 * 
 * Tenor and Maturity are provided by input {@link Map}. Then ,they are not provided as plain columns. IColumnGenerator
 * will play a role.
 * 
 * @author Benoit Lacelle
 */
// https://www.investopedia.com/terms/t/tenor.asp#toc-tenor-vs-maturity
public class TestColumnGenerator_TenorMaturity extends ADagTest implements IExamplePnLExplainConstant {

	@BeforeEach
	@Override
	public void feedTable() {
		table.add(Map.of("sensitivities",
				MarketRiskSensitivity.empty().addDelta(Map.of(K_TENOR, "1Y", K_MATURITY, "2Y"), 12.34D)));
		table.add(Map.of("sensitivities",
				MarketRiskSensitivity.empty().addDelta(Map.of(K_TENOR, "1Y", K_MATURITY, "2Y"), 23.45D)));

		table.add(Map.of("sensitivities",
				MarketRiskSensitivity.empty().addDelta(Map.of(K_TENOR, "3M", K_MATURITY, "1Y"), 34.56D)));
		table.add(Map.of("sensitivities",
				MarketRiskSensitivity.empty().addDelta(Map.of(K_TENOR, "3M", K_MATURITY, "2Y"), 45.67D)));
	}

	@BeforeEach
	public void registerMeasures() {
		forest.addMeasure(Aggregator.builder()
				.name("delta.MERGED")
				.aggregationKey(MarketRiskSensitivityAggregation.class.getName())
				.build());

		// Enable looking at delta along tenor and maturity
		forest.addMeasure(Dispatchor.builder()
				.name("delta")
				.decompositionKey(ExpandTenorAndMaturityDecomposition.class.getName())
				.underlying("delta.MERGED")
				.build());

		forest.addMeasure(Combinator.builder()
				.name("market_shift")
				.combinationKey(MarketDataShiftCombination.class.getName())
				.build());

		forest.addMeasure(Bucketor.builder()
				.name("pnl_explain")
				.groupBy(GroupByColumns.named(K_TENOR, K_MATURITY))
				.combinationKey(EvaluatedExpressionCombination.KEY)
				.combinationOptions(
						Map.of(EvaluatedExpressionCombination.K_EXPRESSION, "underlyings[0] * underlyings[1]"))
				.underlyings(Arrays.asList("delta", "market_shift"))
				.build());
	}

	@Test
	public void testQueryGrandTotal() {
		ITabularView output = cube().execute(CubeQuery.builder().measure("pnl_explain").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						// "k1", 123 + 345, "k2", 234 + 456,
						Map.of("sumK1K2", 0D + 123 + 234 + 345 + 456));
	}

}
