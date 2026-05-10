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
package eu.solven.adhoc.table;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.forest.MeasureForest;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.EmptyMeasure;
import eu.solven.adhoc.model.measure.IMeasure;
import eu.solven.adhoc.model.measure.ReferencedMeasure;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.util.NotYetImplementedException;

public class TestSimpleQueryPod {

	@Test
	public void testForTable_factory_yieldsTableBoundPod() {
		InMemoryTable table = InMemoryTable.builder().name("t1").build();

		IQueryPod pod = SimpleQueryPod.forTable(table);

		Assertions.assertThat(pod.getTable()).isSameAs(table);
		Assertions.assertThat(pod.getOptions()).isEmpty();
		Assertions.assertThat(pod.isCancelled()).isFalse();
		Assertions.assertThat(pod.getCancellationDate()).isNull();
	}

	@Test
	public void testGetQueryId_lazyDefault_derivesFromTableName() {
		InMemoryTable table = InMemoryTable.builder().name("table-xyz").build();

		SimpleQueryPod pod = SimpleQueryPod.builder().table(table).build();

		// No explicit queryId set: lazy default uses the table name.
		Assertions.assertThat(pod.getQueryId().getCube()).isEqualTo("table-xyz");
		Assertions.assertThat(pod.getQueryId().isCubeElseTable()).isFalse();
	}

	@Test
	public void testCancellationListeners_areNoOp() {
		IQueryPod pod = SimpleQueryPod.forTable(InMemoryTable.builder().name("t").build());

		// Should not throw and should not change cancellation state.
		Runnable listener = () -> {
			throw new IllegalStateException("must never fire");
		};
		pod.addCancellationListener(listener);
		pod.removeCancellationListener(listener);

		Assertions.assertThat(pod.isCancelled()).isFalse();
	}

	@Test
	public void testWithTable_replacesTable_andPreservesOptions() {
		InMemoryTable t1 = InMemoryTable.builder().name("t1").build();
		InMemoryTable t2 = InMemoryTable.builder().name("t2").build();

		SimpleQueryPod original = SimpleQueryPod.builder().table(t1).option(StandardQueryOptions.CONCURRENT).build();

		IQueryPod swapped = original.withTable(t2);

		Assertions.assertThat(swapped.getTable()).isSameAs(t2);
		Assertions.assertThat(swapped.getOptions()).contains(StandardQueryOptions.CONCURRENT);
	}

	@Test
	public void testResolveIfRef_unknownMeasure_throwsByDefault() {
		InMemoryTable table = InMemoryTable.builder().name("t").build();
		SimpleQueryPod pod = SimpleQueryPod.builder().table(table).forest(MeasureForest.empty()).build();

		IMeasure ref = ReferencedMeasure.ref("missing");

		Assertions.assertThatThrownBy(() -> pod.resolveIfRef(ref)).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void testResolveIfRef_unknownMeasure_underUnknownAreEmpty_yieldsEmptyMeasure() {
		InMemoryTable table = InMemoryTable.builder().name("t").build();
		SimpleQueryPod pod = SimpleQueryPod.builder()
				.table(table)
				.forest(MeasureForest.empty())
				.option(StandardQueryOptions.UNKNOWN_MEASURES_ARE_EMPTY)
				.build();

		IMeasure resolved = pod.resolveIfRef(ReferencedMeasure.ref("missing"));

		Assertions.assertThat(resolved)
				.isInstanceOfSatisfying(EmptyMeasure.class,
						em -> Assertions.assertThat(em.getName()).isEqualTo("missing"));
	}

	@Test
	public void testResolveIfRef_null_throwsIllegalArgument() {
		IQueryPod pod = SimpleQueryPod.forTable(InMemoryTable.builder().name("t").build());

		Assertions.assertThatThrownBy(() -> pod.resolveIfRef(null)).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void testResolveIfRef_knownMeasure_returnsResolved() {
		Aggregator k1 = Aggregator.sum("k1");
		MeasureForest forest = MeasureForest.builder().name("f").measure(k1).build();

		IQueryPod pod =
				SimpleQueryPod.builder().table(InMemoryTable.builder().name("t").build()).forest(forest).build();

		Assertions.assertThat(pod.resolveIfRef(ReferencedMeasure.ref("k1"))).isEqualTo(k1);
	}

	@Test
	public void testAsTableQuery_throwsNotYetImplemented() {
		IQueryPod pod = SimpleQueryPod.forTable(InMemoryTable.builder().name("t").build());

		Assertions.assertThatThrownBy(pod::asTableQuery).isInstanceOf(NotYetImplementedException.class);
	}
}
