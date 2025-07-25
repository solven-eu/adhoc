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

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.ThrowingCombination;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.measure.combination.EvaluatedExpressionCombination;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.util.AdhocUnsafe;

public class TestCubeQueryEngine extends ADagTest implements IAdhocTestConstants {
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
		String measureA = "m_A";
		String measureB = "m_B";

		Combinator mAIsMbTimed2 = Combinator.builder()
				.name(measureA)
				.underlying(measureB)
				.combinationKey(EvaluatedExpressionCombination.KEY)
				.combinationOptions(ImmutableMap.<String, Object>builder()
						.put("expression", "IF(m_B == null, null, m_B * 2)")
						.build())
				.build();

		Combinator mBIsMaDividedBy2 = Combinator.builder()
				.name(measureB)
				.underlying(measureA)
				.combinationKey(EvaluatedExpressionCombination.KEY)
				.combinationOptions(ImmutableMap.<String, Object>builder()
						.put("expression", "IF(m_A == null, null, m_A / 2)")
						.build())
				.build();

		forest.addMeasure(mAIsMbTimed2);
		forest.addMeasure(mBIsMaDividedBy2);

		Assertions.assertThatThrownBy(() -> cube().execute(CubeQuery.builder().measure(measureA).build()))
				.isInstanceOf(IllegalStateException.class)
				.hasStackTraceContaining("in cycle=");
	}

	@Test
	public void testCompactColumns() {
		CubeQueryEngine engine = engine();

		Aggregator measure = Aggregator.countAsterisk();
		CubeQueryStep step = CubeQueryStep.builder().measure(measure).build();

		IHasUnderlyingMeasures hasUnderlyingMeasures = Mockito.mock(IHasUnderlyingMeasures.class);

		ITransformatorQueryStep queryStep = Mockito.mock(ITransformatorQueryStep.class);
		Mockito.when(hasUnderlyingMeasures.wrapNode(engine.factories, step)).thenReturn(queryStep);

		SliceToValue sliceToValue = Mockito.spy(SliceToValue.empty());
		Mockito.when(queryStep.produceOutputColumn(Mockito.anyList())).thenReturn(sliceToValue);

		ISliceToValue column = engine.processDagStep(Map.of(), step, List.of(), hasUnderlyingMeasures);

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
				.isInstanceOf(IllegalStateException.class)
				.extracting(s -> Throwables.getStackTrace(s))
				.asString()
				// .hasStackTraceContaining does not normalize EOLs
				.containsIgnoringNewLines(
						"""
								Caused by: java.lang.IllegalStateException: Issue computing columns for:
								    (measures) m=m_D given [count(*)]
								    (steps) step=m=m_D filter=matchAll groupBy=grandTotal custom=null given [m=count(*) filter=matchAll groupBy=grandTotal custom=null]
								Path from root:
								\\-CubeQueryStep(id=0, measure=Combinator(name=m_A, tags=[], underlyings=[m_B], combinationKey=COALESCE, combinationOptions={}), filter=matchAll, groupBy=grandTotal, customMarker=null, options=[])
									\\-CubeQueryStep(id=2, measure=Combinator(name=m_B, tags=[], underlyings=[m_C], combinationKey=COALESCE, combinationOptions={}), filter=matchAll, groupBy=grandTotal, customMarker=null, options=[])
										\\-CubeQueryStep(id=4, measure=Combinator(name=m_C, tags=[], underlyings=[m_D], combinationKey=COALESCE, combinationOptions={}), filter=matchAll, groupBy=grandTotal, customMarker=null, options=[])
											\\-CubeQueryStep(id=6, measure=Combinator(name=m_D, tags=[], underlyings=[count(*)], combinationKey=eu.solven.adhoc.measure.ThrowingCombination, combinationOptions={}), filter=matchAll, groupBy=grandTotal, customMarker=null, options=[])""");
	}

}
