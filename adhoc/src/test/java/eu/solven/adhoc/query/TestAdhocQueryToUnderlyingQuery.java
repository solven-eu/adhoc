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
package eu.solven.adhoc.query;

import java.util.Arrays;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.aggregations.sum.SumCombination;
import eu.solven.adhoc.dag.AdhocExecutingQueryContext;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.transformers.Combinator;

public class TestAdhocQueryToUnderlyingQuery extends ADagTest implements IAdhocTestConstants {

	@Override
	public void feedDb() {
		// no need for data
	}

	@Test
	public void testSum() {
		amb.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		amb.addMeasure(k1Sum);
		amb.addMeasure(k2Sum);

		Set<TableQuery> output = aqe.prepareForTable(AdhocExecutingQueryContext.builder()
				.query(AdhocQuery.builder().measure(k1Sum.getName()).build())
				.measures(amb)
				.build());

		Assertions.assertThat(output).hasSize(1).anySatisfy(dbQuery -> {
			Assertions.assertThat(dbQuery.getFilter().isMatchAll()).isTrue();
			Assertions.assertThat(dbQuery.getGroupBy().isGrandTotal()).isTrue();

			Assertions.assertThat(dbQuery.getAggregators()).hasSize(1).contains(k1Sum);
		});
	}

	@Test
	public void testSumOfSum() {
		amb.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		amb.addMeasure(k1Sum);
		amb.addMeasure(k2Sum);

		Set<TableQuery> output = aqe.prepareForTable(AdhocExecutingQueryContext.builder()
				.query(AdhocQuery.builder().measure("sumK1K2").build())
				.measures(amb)
				.build());

		Assertions.assertThat(output).hasSize(1).anySatisfy(dbQuery -> {
			Assertions.assertThat(dbQuery.getFilter().isMatchAll()).isTrue();
			Assertions.assertThat(dbQuery.getGroupBy().isGrandTotal()).isTrue();

			Assertions.assertThat(dbQuery.getAggregators()).hasSize(2).contains(k1Sum, k2Sum);
		});
	}

	@Test
	public void testSum_SumOfSum() {
		amb.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		amb.addMeasure(k1Sum);
		amb.addMeasure(k2Sum);

		Set<TableQuery> output = aqe.prepareForTable(AdhocExecutingQueryContext.builder()
				.query(AdhocQuery.builder().measure("sumK1K2").build())
				.measures(amb)
				.build());

		Assertions.assertThat(output).hasSize(1).anySatisfy(dbQuery -> {
			Assertions.assertThat(dbQuery.getFilter().isMatchAll()).isTrue();
			Assertions.assertThat(dbQuery.getGroupBy().isGrandTotal()).isTrue();

			Assertions.assertThat(dbQuery.getAggregators()).hasSize(2).contains(k1Sum, k2Sum);
		});
	}
}
