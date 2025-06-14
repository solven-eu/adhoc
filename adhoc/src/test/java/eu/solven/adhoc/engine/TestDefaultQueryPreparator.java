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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.context.StandardQueryPreparator;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.UnsafeMeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.cube.AdhocSubQuery;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.table.ITableWrapper;

public class TestDefaultQueryPreparator {
	ITableWrapper table = Mockito.mock(ITableWrapper.class);
	IColumnsManager columnManager = Mockito.mock(IColumnsManager.class);

	UnsafeMeasureForest forest = UnsafeMeasureForest.builder().build();
	{
		Mockito.when(table.getName()).thenReturn("someTableName");
	}

	@Test
	public void testSubQuery() {
		forest.addMeasure(Aggregator.sum("m"));

		StandardQueryPreparator queryPreparator =
				StandardQueryPreparator.builder().implicitFilter(f -> ColumnFilter.isEqualTo("c", "v")).build();

		ICubeQuery query = CubeQuery.builder().measure("m").build();
		AdhocSubQuery subQuery =
				AdhocSubQuery.builder().subQuery(query).parentQueryId(AdhocQueryId.from("someCube", query)).build();

		QueryPod prepared = queryPreparator.prepareQuery(table, forest, columnManager, subQuery);

		Assertions.assertThat(prepared.getQueryId().getParentQueryId())
				.isEqualTo(subQuery.getParentQueryId().getQueryId());
	}

	@Test
	public void test_unknownMeasure() {
		StandardQueryPreparator queryPreparator = StandardQueryPreparator.builder().build();

		ICubeQuery query = CubeQuery.builder().measure("m").build();

		Assertions.assertThatThrownBy(() -> queryPreparator.prepareQuery(table, forest, columnManager, query))
				.isInstanceOf(IllegalArgumentException.class)
				.hasStackTraceContaining("No measure named: m");
	}

	@Test
	public void testCutIrrelevantMeasures() {
		StandardQueryPreparator queryPreparator = StandardQueryPreparator.builder().build();

		forest.addMeasure(Aggregator.sum("c1"));
		forest.addMeasure(Aggregator.sum("c2"));
		forest.addMeasure(Aggregator.sum("c3"));
		forest.addMeasure(Aggregator.sum("c4"));
		forest.addMeasure(Combinator.builder().name("c1+2").underlying("c1").underlying("c2").build());
		forest.addMeasure(Combinator.builder().name("c2+3").underlying("c2").underlying("c3").build());
		forest.addMeasure(Combinator.builder().name("c(1+2)+(2+3)").underlying("c1+2").underlying("c2+3").build());

		{
			ICubeQuery query = CubeQuery.builder().measure("c1").build();
			QueryPod prepared = queryPreparator.prepareQuery(table, forest, columnManager, query);
			Assertions.assertThat(prepared.getForest().getNameToMeasure().keySet()).containsExactly("c1");
			Assertions.assertThat(prepared.getForest().getNameToMeasure().values()).noneSatisfy(m -> {
				Assertions.assertThat(m).isInstanceOf(ReferencedMeasure.class);
			});
		}

		{
			ICubeQuery query = CubeQuery.builder().measure("c1").build();
			QueryPod prepared = queryPreparator.prepareQuery(table, forest, columnManager, query);
			Assertions.assertThat(prepared.getForest().getNameToMeasure().keySet()).containsExactly("c1");
		}

		{
			ICubeQuery query = CubeQuery.builder().measure("c1+2").build();
			QueryPod prepared = queryPreparator.prepareQuery(table, forest, columnManager, query);
			Assertions.assertThat(prepared.getForest().getNameToMeasure().keySet()).containsExactly("c1", "c1+2", "c2");
		}
		{
			ICubeQuery query = CubeQuery.builder().measure("c(1+2)+(2+3)").build();
			QueryPod prepared = queryPreparator.prepareQuery(table, forest, columnManager, query);
			Assertions.assertThat(prepared.getForest().getNameToMeasure().keySet())
					.containsExactly("c(1+2)+(2+3)", "c1", "c1+2", "c2", "c2+3", "c3");
		}
	}

	// The cycle between measure name is not necessarily problematic, as some filter/shift/routing may break such cycles
	// (as CubeQueryStep)
	@Test
	public void testCycleInMeasure() {
		forest.addMeasure(Aggregator.sum("m"));

		forest.addMeasure(Combinator.builder().name("a").underlying("b").build());
		forest.addMeasure(Combinator.builder().name("b").underlying("a").build());

		StandardQueryPreparator queryPreparator = StandardQueryPreparator.builder().build();

		ICubeQuery query = CubeQuery.builder().measure("a").build();

		QueryPod prepared = queryPreparator.prepareQuery(table, forest, columnManager, query);
		Assertions.assertThat(prepared.getForest().getNameToMeasure().keySet()).containsExactly("a", "b");
	}

}
