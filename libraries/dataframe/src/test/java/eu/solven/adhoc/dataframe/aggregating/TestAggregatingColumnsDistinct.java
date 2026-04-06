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
package eu.solven.adhoc.dataframe.aggregating;

import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.openjdk.jol.info.GraphLayout;

import eu.solven.adhoc.collection.FrozenException;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.pepper.memory.PepperFootprintHelper;

public class TestAggregatingColumnsDistinct {
	Aggregator a = Aggregator.sum("a");
	CubeQueryStep step = CubeQueryStep.builder().measure("m").build();

	AggregatingColumnsDistinct<String> aggregatingColumns = AggregatingColumnsDistinct.<String>builder().build();

	@Test
	public void testUnknownKey() {
		aggregatingColumns.contribute("k", a).onLong(123);

		IMultitypeColumnFastGet<String> closedColumn = aggregatingColumns.closeColumn(step, a);

		Assertions.assertThat(IValueProvider.getValue(closedColumn.onValue("k"))).isEqualTo(123L);
		Assertions.assertThat(IValueProvider.getValue(closedColumn.onValue("unknownKey"))).isNull();
	}

	// A slice is opened but no aggregate is ever contributed for it: the column must be empty, no NPE.
	@Test
	public void testSliceOpenedWithNoAggregate() {
		aggregatingColumns.openSlice("k");

		IMultitypeColumnFastGet<String> closedColumn = aggregatingColumns.closeColumn(step, a);

		Assertions.assertThat(closedColumn.isEmpty()).isTrue();
	}

	// One slice has an aggregate, a second slice is opened but receives no aggregate.
	// Querying the second slice must return null, not throw NPE.
	@Test
	public void testMixedSlices_oneWithAggregateOneWithout() {
		aggregatingColumns.contribute("k1", a).onLong(123);
		aggregatingColumns.openSlice("k2");

		IMultitypeColumnFastGet<String> closedColumn = aggregatingColumns.closeColumn(step, a);

		Assertions.assertThat(IValueProvider.getValue(closedColumn.onValue("k1"))).isEqualTo(123L);
		Assertions.assertThat(IValueProvider.getValue(closedColumn.onValue("k2"))).isNull();
	}

	@Test
	public void testNullValue() {
		aggregatingColumns.contribute("k1", a).onObject(null);

		Assertions.assertThat(aggregatingColumns)
				.hasToString("AggregatingColumnsDistinct{#slices=1, aggregators=1, k1={a=null}}");
	}

	@Test
	public void testFrozen_afterCloseColumn() {
		aggregatingColumns.contribute("k1", a).onLong(123);

		Assertions.assertThat(aggregatingColumns.isFrozen()).isFalse();

		aggregatingColumns.closeColumn(step, a);

		Assertions.assertThat(aggregatingColumns.isFrozen()).isTrue();
		Assertions.assertThatThrownBy(() -> aggregatingColumns.openSlice("k2")).isInstanceOf(FrozenException.class);
	}

	@Test
	public void testTwoColumnsShareReversed() {
		Aggregator b = Aggregator.sum("b");

		aggregatingColumns.contribute("k1", a).onLong(123L);
		aggregatingColumns.contribute("k2", b).onLong(234L);

		IMultitypeColumnFastGet<String> closedA =
				aggregatingColumns.closeColumn(CubeQueryStep.builder().measure(a).build(), a);
		IMultitypeColumnFastGet<String> closedB =
				aggregatingColumns.closeColumn(CubeQueryStep.builder().measure(b).build(), b);

		Assertions.assertThat(GraphLayout.parseInstance(closedA).totalSize()).isEqualTo(1616L);
		Assertions.assertThat(GraphLayout.parseInstance(closedB).totalSize()).isEqualTo(1616L);

		// Make sure closedA and closedB shares the `indexToSlice` structure: this must be much smaller than
		// `closedA+closedB`
		Assertions.assertThat(PepperFootprintHelper.deepSize(Arrays.asList(closedA, closedB))).isEqualTo(2440L);
	}

}
