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

import java.util.Locale;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.base.CaseFormat;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.case_insensitivive.CaseInsensitiveCubeQueryEngine;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.ICubeQueryEngine;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.model.Shiftor;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;

/**
 * Verifies that {@link CaseInsensitiveCubeQueryEngine} normalizes filter and groupBy column names to the canonical
 * (schema) casing before execution.
 */
public class TestInMemoryCube_CaseInsensitive_schemaLower extends ADagTest implements IAdhocTestConstants {

	// Schema column is lowercase; queries will use mixed case to exercise normalization
	static final String COLUMN_COUNTRY = "country";

	@Override
	public CubeWrapper.CubeWrapperBuilder makeCube() {
		ICubeQueryEngine caseInsensitiveEngine =
				CaseInsensitiveCubeQueryEngine.builder().delegate(super.engine()).build();
		return super.makeCube().engine(caseInsensitiveEngine);
	}

	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of(COLUMN_COUNTRY, "fr", "k1", 100));
		table().add(Map.of(COLUMN_COUNTRY, "fr", "k1", 200));
		table().add(Map.of(COLUMN_COUNTRY, "de", "k1", 300));

		forest.addMeasure(k1Sum);
	}

	@Test
	public void test_groupBy_mixedCase() {
		// "Country" (capital C) must be normalized to the schema column "country"
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure(k1Sum)
				.groupByAlso(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, COLUMN_COUNTRY))
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(COLUMN_COUNTRY, "fr"), Map.of(k1Sum.getName(), 0L + 100 + 200))
				.containsEntry(Map.of(COLUMN_COUNTRY, "de"), Map.of(k1Sum.getName(), 0L + 300))
				.hasSize(2);
	}

	@Test
	public void test_filter_mixedCase() {
		// "COUNTRY" (all caps) in the filter must be normalized to the schema column "country"
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure(k1Sum)
				.filter(ColumnFilter.matchEq(COLUMN_COUNTRY.toUpperCase(Locale.US), "fr"))
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 100 + 200))
				.hasSize(1);
	}

	@Disabled("TODO CaseInsensitivity")
	@Test
	public void test_shiftorMixedCase() {
		forest.addMeasure(Shiftor.builder()
				.name("frOnly")
				.underlying(k1Sum.getName())
				.lambda(filter -> SimpleFilterEditor.shift(filter, COLUMN_COUNTRY.toUpperCase(Locale.US), "fr"))
				.build());

		// "COUNTRY" (all caps) in the filter must be normalized to the schema column "country"
		ITabularView result = cube().execute(CubeQuery.builder().measure("frOnly").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of("frOnly", 0L + 100 + 200))
				.hasSize(1);
	}
}
