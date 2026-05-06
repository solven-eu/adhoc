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

import org.assertj.core.api.Assertions;
import org.assertj.core.util.Throwables;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.ATestDagInMemory;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.dataframe.column.Cuboid;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.engine.cache.GuavaQueryStepCache;
import eu.solven.adhoc.engine.context.StandardQueryPreparator;
import eu.solven.adhoc.engine.measure.IMeasureQueryStepFactory.IMeasureQueryStepOwnFactory;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.TableQueryEngineFactory;
import eu.solven.adhoc.engine.tabular.optimizer.ITableQueryFactoryFactory;
import eu.solven.adhoc.engine.tabular.optimizer.TableQueryFactory;
import eu.solven.adhoc.measure.ThrowingCombination;
import eu.solven.adhoc.measure.ThrowingCombination.ThrowingCombinationException;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.measure.forest.MeasureForest;
import eu.solven.adhoc.measure.transformator.step.IMeasureQueryStep;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.table.InMemoryTable;
import eu.solven.adhoc.util.AdhocUnsafe;

public class TestDagCubeQueryEngine extends ATestDagInMemory implements IAdhocTestConstants {
	@Override
	public void feedTable() {
		// No need to feed
	}

	@Test
	public void testConflictingNames() {
		Aggregator k1Max = k1Sum.toBuilder().aggregationKey(MaxAggregation.KEY).build();

		Assertions.assertThatThrownBy(() -> cube().execute(CubeQuery.builder().measure(k1Sum, k1Max).build()))
				.isInstanceOf(IllegalArgumentException.class)
				.hasStackTraceContaining("Can not query multiple measures with same name: {k1=2}");
	}

	@Test
	public void testCycleBetweenQuerySteps() {
		AdhocUnsafe.resetDeterministicQueryIds();

		String measureA = "m_A";
		String measureB = "m_B";

		Combinator mAIsMbTimed2 = Combinator.builder().name(measureA).underlying(measureB).build();

		Combinator mBIsMaDividedBy2 = Combinator.builder().name(measureB).underlying(measureA).build();

		forest.addMeasure(mAIsMbTimed2);
		forest.addMeasure(mBIsMaDividedBy2);

		Assertions.assertThatThrownBy(() -> cube().execute(CubeQuery.builder().measure(measureA).build()))
				.isInstanceOf(IllegalStateException.class)
				.hasStackTraceContaining("Adding edge "
						+ "`CubeQueryStep{id=2, measure=Combinator(name=m_B, underlyings=[m_A], combinationKey=SUM)}`"
						+ "->"
						+ "`CubeQueryStep{id=4, measure=Combinator(name=m_A, underlyings=[m_B], combinationKey=SUM)}`"
						+ " would create a cycle: ["
						+ "CubeQueryStep{id=2, measure=Combinator(name=m_B, underlyings=[m_A], combinationKey=SUM)}, "
						+ "CubeQueryStep{id=4, measure=Combinator(name=m_A, underlyings=[m_B], combinationKey=SUM)}, "
						+ "CubeQueryStep{id=2, measure=Combinator(name=m_B, underlyings=[m_A], combinationKey=SUM)}]");
	}

	@Test
	public void testCycleBetweenQuerySteps_referItself() {
		AdhocUnsafe.resetDeterministicQueryIds();
		String measureA = "m_A";

		Combinator mAIsMbTimed2 = Combinator.builder().name(measureA).underlying(measureA).build();

		forest.addMeasure(mAIsMbTimed2);

		Assertions.assertThatThrownBy(() -> cube().execute(CubeQuery.builder().measure(measureA).build()))
				.isInstanceOf(IllegalStateException.class)
				.hasRootCauseMessage("loops not allowed")
				.hasStackTraceContaining("A queryStep can not be its own underlying: "
						+ "`CubeQueryStep{id=0, measure=Combinator(name=m_A, underlyings=[m_A], combinationKey=SUM)}`");
	}

	@Test
	public void testCompactColumns() {
		CubeQueryEngine engine = engine();

		Aggregator measure = Aggregator.countAsterisk();
		CubeQueryStep step = CubeQueryStep.builder().measure(measure).build();

		IMeasureQueryStepOwnFactory hasUnderlyingMeasures = Mockito.mock(IMeasureQueryStepOwnFactory.class);

		IMeasureQueryStep queryStep = Mockito.mock(IMeasureQueryStep.class);
		Mockito.when(hasUnderlyingMeasures.makeQueryStep(engine.factories, step)).thenReturn(queryStep);

		Cuboid sliceToValue = Mockito.spy(Cuboid.empty());
		Mockito.when(queryStep.produceOutputColumn(Mockito.anyList())).thenReturn(sliceToValue);

		ICuboid column = engine.processDagStep(Map.of(), step, List.of(), hasUnderlyingMeasures);

		Mockito.verify(column).compact();
	}

	@Test
	public void testThrowOnDeepMeasure() {
		String measureA = "m_A";
		String measureB = "m_B";
		String measureC = "m_C";
		String measureD = "m_D";

		Combinator mA = Combinator.builder()
				.name(measureA)
				.underlying(measureB)
				.combinationKey(CoalesceCombination.KEY)
				.build();

		Combinator mB = Combinator.builder()
				.name(measureB)
				.underlying(measureC)
				.combinationKey(CoalesceCombination.KEY)
				.build();

		Combinator mC = Combinator.builder()
				.name(measureC)
				.underlying(measureD)
				.combinationKey(CoalesceCombination.KEY)
				.build();

		Combinator mD = Combinator.builder()
				.name(measureD)
				.underlying(Aggregator.countAsterisk().getName())
				.combinationKey(ThrowingCombination.class.getName())
				.build();

		forest.addMeasure(mA);
		forest.addMeasure(mB);
		forest.addMeasure(mC);
		forest.addMeasure(mD);
		forest.addMeasure(Aggregator.countAsterisk());

		table().add(Map.of("a", "a1"));

		AdhocUnsafe.resetDeterministicQueryIds();
		Assertions.assertThatThrownBy(() -> cube().execute(CubeQuery.builder().measure(measureA).build()))
				.isInstanceOf(IllegalArgumentException.class)
				.hasRootCauseInstanceOf(ThrowingCombinationException.class)
				.extracting(s -> Throwables.getStackTrace(s))
				.asString()
				// .hasStackTraceContaining does not normalize EOLs
				.containsIgnoringNewLines(
						"""
								Caused by: java.lang.IllegalStateException: Issue computing columns for:
								    (measures) m=m_D given [count(*)]
								    (steps) step=m=m_D filter=matchAll groupBy=grandTotal custom=null given [m=count(*) filter=matchAll groupBy=grandTotal custom=null]
								Path from root:
								\\-CubeQueryStep{id=0, measure=Combinator(name=m_A, underlyings=[m_B], combinationKey=COALESCE)}
									\\-CubeQueryStep{id=2, measure=Combinator(name=m_B, underlyings=[m_C], combinationKey=COALESCE)}
										\\-CubeQueryStep{id=4, measure=Combinator(name=m_C, underlyings=[m_D], combinationKey=COALESCE)}
											\\-CubeQueryStep{id=6, measure=Combinator(name=m_D, underlyings=[count(*)], combinationKey=eu.solven.adhoc.measure.ThrowingCombination)}""");
	}

	// Check the API to customize the TableQueryEngine and especially the TableQueryEngineOptimizer is actually valid.
	@Test
	public void testCustomTableQueryOptimizer() {
		ITableQueryFactoryFactory queryFactoryFactory = (filterBundle, hasOptions) -> {
			return TableQueryFactory.builder()
					.filterBundle(filterBundle)
					.splitForAdhocInference(filterBundle)
					.groupByAggregator()
					.build();
		};
		CubeQueryEngine cubeEngine = CubeQueryEngine.builder()
				.tableQueryEngine(TableQueryEngineFactory.builder().queryFactoryFactory(queryFactoryFactory).build())
				.build();

		Assertions.assertThat(cubeEngine.getTableQueryEngine()).isNotNull();
	}

	@Test
	public void testQueryStepCache() {
		CubeWrapper cubeWrapper = CubeWrapper.builder()
				.table(InMemoryTable.builder().build())
				.forest(MeasureForest.empty())
				.queryPreparator(
						StandardQueryPreparator.builder().queryStepCache(GuavaQueryStepCache.withSize(1024)).build())
				.build();

		Assertions.assertThat(cubeWrapper.getTable()).isNotNull();
	}

	@Test
	public void testUnsafeExecute() {
		CubeQueryEngine engine = CubeQueryEngine.builder().build();

		{
			ITabularView view = engine.executeUnsafe(CubeQuery.builder().build(), forest, table());
			Assertions.assertThat(view.isEmpty()).isTrue();
		}

		{
			ITabularView view = engine
					.executeUnsafe(CubeQuery.builder().build(), forest, table(), ColumnsManager.builder().build());
			Assertions.assertThat(view.isEmpty()).isTrue();
		}
	}

}
