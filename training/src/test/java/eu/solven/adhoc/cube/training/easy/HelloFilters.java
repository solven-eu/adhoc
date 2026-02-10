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
package eu.solven.adhoc.cube.training.easy;

import java.util.Map;
import java.util.regex.Pattern;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.value.ComparingMatcher;
import eu.solven.adhoc.table.InMemoryTable;

public class HelloFilters {
	@Test
	public void helloFilters() {
		Assertions.assertThat(FilterBuilder.or()
				.filter(ColumnFilter.match("a", ComparingMatcher.greaterThanOrEqual(123L)))
				.filter(ColumnFilter.match("b", ComparingMatcher.strictlyLowerThan(234L)))
				// `.combine` will not apply optimization (but trivial ones)
				.combine()).hasToString("a>=123|b<234");
	}

	@Test
	public void helloFiltersLike() {
		Assertions.assertThat(ColumnFilter.matchLike("a", "pre%")).hasToString("a LIKE 'pre%'");
		Assertions.assertThat(ColumnFilter.matchPattern("a", Pattern.compile("pre.*")))
				.hasToString("a matches `RegexMatcher(pattern=pre.*)`");
	}

	@Test
	public void helloFilterBuilderOptimizations() {
		Assertions.assertThat(FilterBuilder.or()
				.filter(ColumnFilter.match("a", ComparingMatcher.greaterThanOrEqual(123L)))
				.filter(ColumnFilter.match("b", ComparingMatcher.strictlyLowerThan(234L)))
				// `.combine` will not apply optimization (but trivial ones)
				.combine()).hasToString("a>=123|b<234");

		Assertions.assertThat(FilterBuilder.or()
				.filter(ColumnFilter.match("a", ComparingMatcher.greaterThanOrEqual(123L)))
				.filter(ColumnFilter.match("b", ComparingMatcher.strictlyLowerThan(234L)))
				.filter(ColumnFilter.matchEq("a", 345L))
				// `.optimize` will simplify boolean expressions
				.optimize()).hasToString("b<234|a>=123");
	}

	@Test
	public void helloCustomFilter() {
		InMemoryTable table = InMemoryTable.builder().build();
		table.add(ImmutableMap.of("a", "a1", "b", "b1", "v1", 1, "v2", 2));
		table.add(ImmutableMap.of("a", "a1", "b", "b2", "v2", 7));
		table.add(ImmutableMap.of("a", "a2", "b", "b1", "v1", 11));

		IMeasure sum = Combinator.sum("v1", "v2");
		MeasureForest forest = MeasureForest.builder()
				.name("someForest")
				.measure(Aggregator.sum("v1"))
				.measure(Aggregator.sum("v2"))
				.measure(sum)
				.build();

		ICubeWrapper cube = CubeWrapper.builder().table(table).forest(forest).build();

		ISliceFilter customFilter = ColumnFilter.match("a", someA -> {
			return someA instanceof String someAString && someAString.endsWith("1");
		});

		ICubeQuery query = CubeQuery.builder().groupByAlso("a").measure(sum.getName()).filter(customFilter).build();

		ITabularView result = cube.execute(query);
		MapBasedTabularView resultListBased = MapBasedTabularView.load(result);

		Assertions.assertThat(resultListBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of("a", "a1"), Map.of(sum.getName(), 1L + 2L + 7L));
	}
}
