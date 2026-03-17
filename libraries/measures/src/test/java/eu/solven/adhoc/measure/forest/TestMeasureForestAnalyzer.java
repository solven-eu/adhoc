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
package eu.solven.adhoc.measure.forest;

import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.SetMultimap;

import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.Shiftor;

public class TestMeasureForestAnalyzer {

	// ── columnToSpecialValues ─────────────────────────────────────────────────

	// An Aggregator contributes its columnName with no special values.
	@Test
	public void testAggregator_columnPresentNoSpecialValues() {
		IMeasureForest forest = MeasureForest.builder().name("f").measure(Aggregator.sum("revenue")).build();

		Map<String, Set<Object>> result = MeasureForestAnalyzer.columnToSpecialValues(forest);

		Assertions.assertThat(result).containsOnlyKeys("revenue");
		Assertions.assertThat(result.get("revenue")).isEmpty();
	}

	// A Filtrator with an equality filter contributes its column and the equality operand as a special value.
	@Test
	public void testFiltrator_equalsFilter_specialValue() {
		Filtrator filtered = Filtrator.builder()
				.name("eurRevenue")
				.underlying("revenue")
				.filter(ColumnFilter.matchEq("ccy", "EUR"))
				.build();
		IMeasureForest forest =
				MeasureForest.builder().name("f").measure(Aggregator.sum("revenue")).measure(filtered).build();

		Map<String, Set<Object>> result = MeasureForestAnalyzer.columnToSpecialValues(forest);

		Assertions.assertThat(result).containsKey("ccy");
		Assertions.assertThat(result.get("ccy")).containsExactly("EUR");
	}

	// An IN filter contributes all operands as special values.
	@Test
	public void testFiltrator_inFilter_multipleSpecialValues() {
		Filtrator filtered = Filtrator.builder()
				.name("multiCcy")
				.underlying("revenue")
				.filter(ColumnFilter.matchIn("ccy", "EUR", "USD"))
				.build();
		IMeasureForest forest =
				MeasureForest.builder().name("f").measure(Aggregator.sum("revenue")).measure(filtered).build();

		Map<String, Set<Object>> result = MeasureForestAnalyzer.columnToSpecialValues(forest);

		Assertions.assertThat(result.get("ccy")).containsExactlyInAnyOrder("EUR", "USD");
	}

	// A composite filter (AND of two column filters) contributes both columns.
	@Test
	public void testFiltrator_compositeFilter_bothColumnsPresent() {
		Filtrator filtered = Filtrator.builder()
				.name("filtered")
				.underlying("revenue")
				.filter(AndFilter.and(ColumnFilter.matchEq("ccy", "EUR"), ColumnFilter.matchEq("region", "EMEA")))
				.build();
		IMeasureForest forest =
				MeasureForest.builder().name("f").measure(Aggregator.sum("revenue")).measure(filtered).build();

		Map<String, Set<Object>> result = MeasureForestAnalyzer.columnToSpecialValues(forest);

		Assertions.assertThat(result).containsKeys("ccy", "region");
		Assertions.assertThat(result.get("ccy")).containsExactly("EUR");
		Assertions.assertThat(result.get("region")).containsExactly("EMEA");
	}

	// A Shiftor contributes its editorOptions keys as columns (no special values from options).
	@Test
	public void testShiftor_optionKeys_appearsAsColumns() {
		Shiftor shiftor = Shiftor.builder().name("shifted").underlying("revenue").editorOption("country", "FR").build();
		IMeasureForest forest =
				MeasureForest.builder().name("f").measure(Aggregator.sum("revenue")).measure(shiftor).build();

		Map<String, Set<Object>> result = MeasureForestAnalyzer.columnToSpecialValues(forest);

		Assertions.assertThat(result).containsKey("country");
	}

	// ── tagToColumns ─────────────────────────────────────────────────────────

	// A forest with no tags produces an empty multimap.
	@Test
	public void testTagToColumns_noTags_emptyMultimap() {
		IMeasureForest forest = MeasureForest.builder().name("f").measure(Aggregator.sum("revenue")).build();

		SetMultimap<String, String> result = MeasureForestAnalyzer.tagToColumns(forest);

		Assertions.assertThat(result.asMap()).isEmpty();
	}

	// A tagged Aggregator maps its tag to its columnName.
	@Test
	public void testTagToColumns_taggedAggregator_columnMapped() {
		Aggregator agg = Aggregator.builder().name("revenue").columnName("rev_col").tag("pnl").build();
		IMeasureForest forest = MeasureForest.builder().name("f").measure(agg).build();

		SetMultimap<String, String> result = MeasureForestAnalyzer.tagToColumns(forest);

		Assertions.assertThat(result.get("pnl")).containsExactly("rev_col");
	}

	// Two measures share the same tag but reference different columns — both appear under that tag.
	@Test
	public void testTagToColumns_twoMeasuresSameTag_bothColumnsMapped() {
		Aggregator agg1 = Aggregator.builder().name("m1").columnName("col1").tag("team-a").build();
		Aggregator agg2 = Aggregator.builder().name("m2").columnName("col2").tag("team-a").build();
		IMeasureForest forest = MeasureForest.builder().name("f").measure(agg1).measure(agg2).build();

		SetMultimap<String, String> result = MeasureForestAnalyzer.tagToColumns(forest);

		Assertions.assertThat(result.get("team-a")).containsExactlyInAnyOrder("col1", "col2");
	}

	// A tagged Filtrator maps its tag to every column referenced inside the filter.
	@Test
	public void testTagToColumns_taggedFiltrator_filterColumnsIncluded() {
		Filtrator filtered = Filtrator.builder()
				.name("eurRevenue")
				.underlying("revenue")
				.filter(ColumnFilter.matchEq("ccy", "EUR"))
				.tag("finance")
				.build();
		IMeasureForest forest =
				MeasureForest.builder().name("f").measure(Aggregator.sum("revenue")).measure(filtered).build();

		SetMultimap<String, String> result = MeasureForestAnalyzer.tagToColumns(forest);

		// Filtrator references the "ccy" column via its filter
		Assertions.assertThat(result.get("finance")).contains("ccy");
	}
}
