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
package eu.solven.adhoc.query.duplicating;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.decomposition.CumulatingDecomposition;
import eu.solven.adhoc.measure.decomposition.DuplicatingDecomposition;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.value.ComparingMatcher;

public class TestCubeQuery_CumulatingDecomposition extends ADagTest implements IAdhocTestConstants {

	final String dispatchedMeasure = "k1.dispatched";

	final String cElement = "country";
	final String cGroup = "country_groups";

	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of("l", "A", "d", 0, "k1", 123));
		table().add(Map.of("l", "B", "d", 1, "k1", 234));
		table().add(Map.of("l", "C", "d", 2, "k1", 345));
	}

	protected String getDecompositionKey() {
		return CumulatingDecomposition.class.getName();
	}

	@SuppressWarnings({ "checkstyle:AvoidInlineConditionals" })
	protected Function<Object, Object> upToFunction() {
		return t -> t instanceof Comparable<?> c
				? ComparingMatcher.builder().greaterThan(false).matchIfEqual(true).operand(c).build()
				: t;
	}

	@SuppressWarnings({ "checkstyle:AvoidInlineConditionals" })
	protected IFilterEditor upToEditor() {
		return f -> SimpleFilterEditor.shiftIfPresent(f, "d", upToFunction());
	}

	@BeforeEach
	void prepareMeasures() {
		forest.addMeasure(Dispatchor.builder()
				.name(dispatchedMeasure)
				.underlying("k1")
				.decompositionKey(getDecompositionKey())
				.decompositionOptions(Map.of(DuplicatingDecomposition.K_COLUMN_TO_COORDINATES,
						ImmutableMap.builder().put("d", Set.of()).build()))
				.decompositionOption(CumulatingDecomposition.K_FILTER_EDITOR, upToEditor())
				.build());

		forest.addMeasure(k1Sum);
	}

	@Test
	public void testGrandTotal() {
		ITabularView output = cube().execute(CubeQuery.builder().measure(dispatchedMeasure).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 123 + 234 + 345));
	}

	@Test
	public void testGrandTotal_filterDate() {
		ITabularView output = cube().execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter("d", 1).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 123 + 234));
	}

	@Test
	public void testGrandTotal_filterUnknownDate() {
		ITabularView output =
				cube().execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter("d", -666).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).isEmpty();
	}

	// BEWARE One may argue we want to cumulate skipping `d==1`.
	@Test
	public void testGrandTotal_filterTwoDates() {
		ITabularView output =
				cube().execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter("d", Set.of(0, 2)).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 123 + 234 + 345));
	}

	@Test
	public void testGroupByDate() {
		ITabularView output = cube().execute(CubeQuery.builder().measure(dispatchedMeasure).groupByAlso("d").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(3)
				.containsEntry(Map.of("d", 0L), Map.of(dispatchedMeasure, 0L + 123))
				.containsEntry(Map.of("d", 1L), Map.of(dispatchedMeasure, 0L + 123 + 234))
				.containsEntry(Map.of("d", 2L), Map.of(dispatchedMeasure, 0L + 123 + 234 + 345));
	}

	@Test
	public void testGroupByDate_filterDate() {
		ITabularView output = cube()
				.execute(CubeQuery.builder().measure(dispatchedMeasure).groupByAlso("d").andFilter("d", 1).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of("d", 1L), Map.of(dispatchedMeasure, 0L + 123 + 234));
	}
}
