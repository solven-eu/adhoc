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
package eu.solven.adhoc.engine;

import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.engine.cache.IQueryStepCache;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.IWhereGroupByQuery;
import eu.solven.adhoc.factories.AdhocFactories;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.forest.IMeasureResolver;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.query.IGroupBy;

/**
 * Unit tests for {@link QueryStepsDagBuilder#registerUnderlying}: verifies that self-loops, direct cycles, and indirect
 * cycles all produce targeted, readable error messages via {@link QueryStepsDagBuilder#buildAddEdgeException}.
 *
 * @author Benoit Lacelle
 */
public class TestQueryStepsDagBuilder {

	AdhocFactories factories = AdhocFactories.builder().build();
	IQueryStepCache cache = Mockito.mock(IQueryStepCache.class);
	IMeasureResolver measureResolver = Mockito.mock(IMeasureResolver.class);
	IWhereGroupByQuery query = Mockito.mock(IWhereGroupByQuery.class);

	QueryStepsDagBuilder builder;

	CubeQueryStep stepA = CubeQueryStep.builder()
			.filter(ISliceFilter.MATCH_ALL)
			.groupBy(IGroupBy.GRAND_TOTAL)
			.measure(Aggregator.sum("a"))
			.build();

	CubeQueryStep stepB = CubeQueryStep.builder()
			.filter(ISliceFilter.MATCH_ALL)
			.groupBy(IGroupBy.GRAND_TOTAL)
			.measure(Aggregator.sum("b"))
			.build();

	CubeQueryStep stepC = CubeQueryStep.builder()
			.filter(ISliceFilter.MATCH_ALL)
			.groupBy(IGroupBy.GRAND_TOTAL)
			.measure(Aggregator.sum("c"))
			.build();

	@BeforeEach
	void setUp() {
		Mockito.when(cache.getValue(Mockito.any())).thenReturn(Optional.empty());
		builder = new QueryStepsDagBuilder(factories, "testCube", measureResolver, query, cache);
	}

	/**
	 * A step whose underlying is itself produces an explicit self-loop message.
	 */
	@Test
	public void testRegisterUnderlying_selfLoop() {
		builder.addVertex(stepA);

		Assertions.assertThatThrownBy(() -> builder.registerUnderlying(stepA, stepA))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("own underlying")
				.hasMessageContaining(stepA.getMeasure().getName());
	}

	/**
	 * When A→B already exists and B→A is attempted, the message identifies the cycle [B, A, B].
	 */
	@Test
	public void testRegisterUnderlying_directCycle() {
		builder.addVertex(stepA);
		builder.registerUnderlying(stepA, stepB);

		Assertions.assertThatThrownBy(() -> builder.registerUnderlying(stepB, stepA))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("cycle")
				.hasMessageContaining(stepA.getMeasure().getName())
				.hasMessageContaining(stepB.getMeasure().getName());
	}

	/**
	 * When A→B→C already exists and C→A is attempted, the message reports the full cycle [C, A, B, C].
	 */
	@Test
	public void testRegisterUnderlying_indirectCycle() {
		builder.addVertex(stepA);
		builder.registerUnderlying(stepA, stepB);
		builder.registerUnderlying(stepB, stepC);

		Assertions.assertThatThrownBy(() -> builder.registerUnderlying(stepC, stepA))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("cycle")
				.hasMessageContaining(stepA.getMeasure().getName())
				.hasMessageContaining(stepB.getMeasure().getName())
				.hasMessageContaining(stepC.getMeasure().getName());
	}
}
