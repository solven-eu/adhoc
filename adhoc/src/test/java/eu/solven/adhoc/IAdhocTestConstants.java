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
package eu.solven.adhoc;

import java.util.Arrays;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.aggregations.ExpressionCombination;
import eu.solven.adhoc.aggregations.max.MaxCombination;
import eu.solven.adhoc.aggregations.sum.CountAggregator;
import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Bucketor;
import eu.solven.adhoc.transformers.Combinator;
import eu.solven.adhoc.transformers.Dispatchor;
import eu.solven.adhoc.transformers.Filtrator;

public interface IAdhocTestConstants {
	Aggregator k1Sum = Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build();

	Combinator k1SumSquared = Combinator.builder()
			.name("k1SumSquared")
			.underlying(k1Sum.getName())
			.combinationKey(ExpressionCombination.KEY)
			// https://github.com/ezylang/EvalEx/issues/204
			// We may process ternary into IF
			// "k1 == null ? 0 : k1 + k2 == null ? 0 : k2"
			.combinationOptions(
					ImmutableMap.<String, Object>builder().put("expression", "IF(k1 == null, 0, k1 * k1)").build())
			.build();

	Aggregator k2Sum = Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build();

	Combinator k1PlusK2AsExpr = Combinator.builder()
			.name("k1PlusK2AsExpr")
			.underlyings(Arrays.asList("k1", "k2"))
			.combinationKey(ExpressionCombination.KEY)
			// https://github.com/ezylang/EvalEx/issues/204
			// We may process ternary into IF
			// "k1 == null ? 0 : k1 + k2 == null ? 0 : k2"
			.combinationOptions(ImmutableMap.<String, Object>builder()
					.put("expression", "IF(k1 == null, 0, k1) + IF(k2 == null, 0, k2)")
					.build())
			.build();

	Filtrator filterK1onA1 =
			Filtrator.builder().name("filterK1onA1").underlying("k1").filter(ColumnFilter.isEqualTo("a", "a1")).build();

	Bucketor sum_MaxK1K2ByA = Bucketor.builder()
			.name("sum_maxK1K2ByA")
			.underlyings(Arrays.asList("k1", "k2"))
			.groupBy(GroupByColumns.named("a"))
			.combinationKey(MaxCombination.KEY)
			.aggregationKey(SumAggregator.KEY)
			.build();

	Dispatchor dispatchFrom0To100 = Dispatchor.builder()
			.name("0or100")
			.underlying("k1")
			.decompositionKey("linear")
			// ImmutableMap for ordering (e.g. useful for serialization tests)
			.decompositionOptions(ImmutableMap.of("input", "percent", "min", 0, "max", 100, "output", "0_or_100"))
			.aggregationKey(SumAggregator.KEY)
			.build();

	Aggregator countAsterisk = Aggregator.builder()
			.name("countAsterisk")
			.aggregationKey(CountAggregator.KEY)
			.columnName(CountAggregator.ASTERISK)
			.build();
}
