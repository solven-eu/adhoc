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
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.aggregation.comparable.MaxCombination;
import eu.solven.adhoc.measure.combination.ExpressionCombination;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Bucketor;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.Shiftor;
import eu.solven.adhoc.measure.model.Unfiltrator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public interface IAdhocTestConstants {
	Aggregator k1Sum = Aggregator.builder().name("k1").aggregationKey(SumAggregation.KEY).build();

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

	Aggregator k2Sum = Aggregator.builder().name("k2").aggregationKey(SumAggregation.KEY).build();

	Combinator k1PlusK2AsExpr = Combinator.builder()
			.name("k1PlusK2AsExpr")
			.underlyings(Arrays.asList("k1", "k2"))
			.combinationKey(ExpressionCombination.KEY)
			// https://github.com/ezylang/EvalEx/issues/204
			// We may process ternary into IF
			// "k1 == null ? 0 : k1 + k2 == null ? 0 : k2"
			.combinationOptions(ImmutableMap.<String, Object>builder()
					.put(ExpressionCombination.KEY_EXPRESSION, "IF(k1 == null, 0, k1) + IF(k2 == null, 0, k2)")
					.build())
			.build();

	Filtrator filterK1onA1 =
			Filtrator.builder().name("filterK1onA1").underlying("k1").filter(ColumnFilter.isEqualTo("a", "a1")).build();
	Filtrator filterK1onB1 =
			Filtrator.builder().name("filterK1onB1").underlying("k1").filter(ColumnFilter.isEqualTo("b", "b1")).build();

	Unfiltrator unfilterOnA = Unfiltrator.builder().name("unfilterOnK1").underlying("k1").column("a").build();

	Shiftor shiftorAisA1 = Shiftor.builder()
			.name("shiftorAisA1")
			.underlying("k1")
			.editorKey(SimpleFilterEditor.KEY)
			.editorOptions(Map.of(SimpleFilterEditor.P_SHIFTED, Map.of("a", "a1")))
			.build();

	Bucketor sum_MaxK1K2ByA = Bucketor.builder()
			.name("sum_maxK1K2ByA")
			.underlyings(Arrays.asList("k1", "k2"))
			.groupBy(GroupByColumns.named("a"))
			.combinationKey(MaxCombination.KEY)
			.aggregationKey(SumAggregation.KEY)
			.build();

	Dispatchor dispatchFrom0To100 = Dispatchor.builder()
			.name("0or100")
			.underlying("k1")
			.decompositionKey("linear")
			// ImmutableMap for ordering (e.g. useful for serialization tests)
			.decompositionOptions(ImmutableMap.of("input", "percent", "min", 0, "max", 100, "output", "0_or_100"))
			.aggregationKey(SumAggregation.KEY)
			.build();

	Aggregator countAsterisk = Aggregator.countAsterisk();

	/**
	 * A random {@link IValueMatcher} used to test behavior of custom valueMatcher
	 */
	IValueMatcher randomMatcher = FilterHelpers.wrapWithToString(o -> Objects.hash(o) % 2 == 0, () -> "random2");
}
