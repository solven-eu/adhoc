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
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.column.coordinate.CalculatedCoordinate;
import eu.solven.adhoc.engine.cache.IQueryStepCache;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.IWhereGroupByQuery;
import eu.solven.adhoc.factories.AdhocFactories;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.forest.IMeasureResolver;
import eu.solven.adhoc.model.column.ColumnWithCalculatedCoordinates;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.query.IGroupBy;
import eu.solven.adhoc.model.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.MeasurelessQuery;

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

	/**
	 * When a groupBy column declares {@link CalculatedCoordinate}s, the natural sub-query for that column must be
	 * filtered to exclude any row whose coordinate value matches one of the declared calculated- coordinate names.
	 * Without this exclusion the natural and calculated rows would collide on the same slice key and crash the
	 * downstream merge in {@code MapBasedTabularView.appendSlice}. This test pins the wiring at the
	 * {@link QueryStepsDagBuilder#rootMeasureless} level so a future change that drops or reshapes the {@code NOT IN}
	 * filter is caught here rather than via the slower DuckDB cube tests.
	 */
	@Test
	public void testRootMeasureless_calculatedCoordinatesAddNotInFilterToNaturalSubQuery() {
		MeasurelessQuery measureless = MeasurelessQuery.builder()
				.filter(ISliceFilter.MATCH_ALL)
				.groupBy(GroupByColumns.of(ColumnWithCalculatedCoordinates.builder()
						.column("color")
						.calculatedCoordinate(CalculatedCoordinate.builder()
								.coordinate("blue")
								.filter(ColumnFilter.matchEq("d", "today"))
								.build())
						.build()))
				.build();
		QueryStepsDagBuilder localBuilder =
				new QueryStepsDagBuilder(factories, "testCube", measureResolver, measureless, cache);

		Set<MeasurelessQuery> subQueries = localBuilder.rootMeasureless();

		// 2 sub-queries: the natural one (groupBy=(color)) and the calculated `blue` one.
		Assertions.assertThat(subQueries).hasSize(2);

		// The natural sub-query carries `color NOT IN (blue)` so the conflicting natural row is suppressed.
		Assertions.assertThat(subQueries).anySatisfy(mq -> {
			Assertions.assertThat(mq.getGroupBy().toString()).isEqualTo("(color)");
			Assertions.assertThat(mq.getFilter().toString()).isEqualTo("color!=blue");
		})
				// The calculated sub-query carries the declared filter (d == today) and groups on the
				// constant-value FunctionCalculatedColumn that emits "blue" for every matching row.
				.anySatisfy(mq -> {
					Assertions.assertThat(mq.getGroupBy().toString())
							.contains("FunctionCalculatedColumn")
							.contains("recordToCoordinate=blue");
					Assertions.assertThat(mq.getFilter().toString()).isEqualTo("d==today");
				});
	}

	/**
	 * The legacy grand-total marker {@code *} is excluded from the natural-suppression set. Filtering on
	 * {@code col != "*"} would have no semantic effect against real data (real coordinate values never carry the
	 * literal string {@code "*"}) but would crash typed-column SQL backends on the cast that JOOQ generates (e.g.
	 * DuckDB throws "invalid date field format" when comparing a DATE column to the string {@code "*"}). Pinning the
	 * skip rule here ensures legacy {@code CalculatedCoordinate.star()} keeps producing a {@code MATCH_ALL} natural
	 * filter on every column type.
	 */
	@Test
	public void testRootMeasureless_calculatedCoordinateStarIsExcludedFromSuppression() {
		MeasurelessQuery measureless = MeasurelessQuery.builder()
				.filter(ISliceFilter.MATCH_ALL)
				.groupBy(GroupByColumns.of(ColumnWithCalculatedCoordinates.builder()
						.column("d")
						.calculatedCoordinate(CalculatedCoordinate.star())
						.build()))
				.build();
		QueryStepsDagBuilder localBuilder =
				new QueryStepsDagBuilder(factories, "testCube", measureResolver, measureless, cache);

		Set<MeasurelessQuery> subQueries = localBuilder.rootMeasureless();

		Assertions.assertThat(subQueries).hasSize(2);
		Assertions.assertThat(subQueries).anySatisfy(mq -> {
			Assertions.assertThat(mq.getGroupBy().toString()).isEqualTo("(d)");
			Assertions.assertThat(mq.getFilter()).isEqualTo(ISliceFilter.MATCH_ALL);
		}).anySatisfy(mq -> Assertions.assertThat(mq.getFilter()).isEqualTo(ISliceFilter.MATCH_ALL));
	}
}
