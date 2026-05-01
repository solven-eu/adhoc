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
package eu.solven.adhoc.measure.decomposition.many2many;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.cuboid.slice.SliceHelpers;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.engine.step.SliceAsMapWithStep;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.decomposition.IDecompositionEntry;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestManyToMany1DDecomposition {

	static final String ELEMENT_COL = "country";
	static final String GROUP_COL = "countryGroup";

	ManyToMany1DInMemoryDefinition definition = new ManyToMany1DInMemoryDefinition();
	ManyToMany1DDecomposition decomposition;

	@BeforeEach
	public void setUp() {
		decomposition = new ManyToMany1DDecomposition(ImmutableMap.<String, Object>builder()
				.put(ManyToMany1DDecomposition.K_INPUT, ELEMENT_COL)
				.put(ManyToMany1DDecomposition.K_OUTPUT, GROUP_COL)
				.build(), definition);

		definition.putElementToGroup("FR", "G8");
		definition.putElementToGroup("FR", "G20");
		definition.putElementToGroup("DE", "G20");
	}

	private ISliceWithStep makeSlice(Map<String, Object> sliceMap, ISliceFilter filter, boolean groupByGroup) {
		CubeQueryStep step = CubeQueryStep.builder()
				.measure("m")
				.filter(filter)
				.groupBy(groupByGroup ? GroupByColumns.named(GROUP_COL) : GroupByColumns.grandTotal())
				.build();
		return SliceAsMapWithStep.builder().slice(SliceHelpers.asSlice(sliceMap)).queryStep(step).build();
	}

	@Test
	public void testDecompose_noElementInSlice_returnsIdentity() {
		// When the slice has no value for the element column, return a single entry with no coordinate change
		ISliceWithStep slice = makeSlice(Map.of(), ISliceFilter.MATCH_ALL, false);

		List<IDecompositionEntry> entries = decomposition.decompose(slice, 100);

		Assertions.assertThat(entries).hasSize(1);
		Assertions.assertThat(entries.get(0).getSlice()).isEmpty();
		Assertions.assertThat(IValueProvider.getValue(entries.get(0).getValue())).isEqualTo(100);
	}

	@Test
	public void testDecompose_withElement_groupedByGroup_producesOneEntryPerGroup() {
		ISliceWithStep slice = makeSlice(Map.of(ELEMENT_COL, "FR"), ColumnFilter.matchEq(ELEMENT_COL, "FR"), true);

		List<IDecompositionEntry> entries = decomposition.decompose(slice, 200);

		// FR belongs to G8 and G20
		Assertions.assertThat(entries).hasSize(2);
		Set<Object> groups = entries.stream().map(e -> e.getSlice().get(GROUP_COL)).collect(Collectors.toSet());
		Assertions.assertThat(groups).containsExactlyInAnyOrder("G8", "G20");
		entries.forEach(e -> Assertions.assertThat(IValueProvider.getValue(e.getValue())).isEqualTo(200));
	}

	@Test
	public void testDecompose_withElement_notGroupedByGroup_producesSingleEntry() {
		// GroupBy doesn't include the group column → single aggregated contribution
		ISliceWithStep slice = makeSlice(Map.of(ELEMENT_COL, "FR"), ColumnFilter.matchEq(ELEMENT_COL, "FR"), false);

		List<IDecompositionEntry> entries = decomposition.decompose(slice, 300);

		Assertions.assertThat(entries).hasSize(1);
		Assertions.assertThat(entries.get(0).getSlice()).isEmpty();
		Assertions.assertThat(IValueProvider.getValue(entries.get(0).getValue())).isEqualTo(300);
	}

	// --- convertGroupsToElementsFilter ---

	@Test
	public void testConvertFilter_matchAll_passthrough() {
		ISliceFilter result =
				decomposition.convertGroupsToElementsFilter(GROUP_COL, ELEMENT_COL, ISliceFilter.MATCH_ALL);

		Assertions.assertThat(result.isMatchAll()).isTrue();
	}

	@Test
	public void testConvertFilter_columnFilter_onGroupCol_convertsToElementFilter() {
		// A filter on the group column should be converted to a filter on the element column
		ISliceFilter groupFilter = ColumnFilter.matchEq(GROUP_COL, "G8");

		ISliceFilter result = decomposition.convertGroupsToElementsFilter(GROUP_COL, ELEMENT_COL, groupFilter);

		Assertions.assertThat(result.isColumnFilter()).isTrue();
	}

	@Test
	public void testConvertFilter_columnFilter_onOtherCol_passthrough() {
		ISliceFilter otherFilter = ColumnFilter.matchEq("otherColumn", "someValue");

		ISliceFilter result = decomposition.convertGroupsToElementsFilter(GROUP_COL, ELEMENT_COL, otherFilter);

		Assertions.assertThat(result).isSameAs(otherFilter);
	}

	@Test
	public void testConvertFilter_andFilter_convertsEachOperand() {
		ISliceFilter andFilter =
				FilterBuilder.and(List.of(ColumnFilter.matchEq(GROUP_COL, "G8"), ColumnFilter.matchEq("other", "v")))
						.optimize();

		ISliceFilter result = decomposition.convertGroupsToElementsFilter(GROUP_COL, ELEMENT_COL, andFilter);

		Assertions.assertThat(result).isNotNull();
	}

	@Test
	public void testConvertFilter_orFilter_convertsEachOperand() {
		ISliceFilter orFilter =
				FilterBuilder.or(List.of(ColumnFilter.matchEq(GROUP_COL, "G8"), ColumnFilter.matchEq(GROUP_COL, "G20")))
						.optimize();

		ISliceFilter result = decomposition.convertGroupsToElementsFilter(GROUP_COL, ELEMENT_COL, orFilter);

		Assertions.assertThat(result).isNotNull();
	}

	@Test
	public void testGetColumnTypes_returnsGroupColumn() {
		Map<String, Class<?>> columnTypes = decomposition.getColumnTypes();

		Assertions.assertThat(columnTypes).containsKey(GROUP_COL);
	}

	@Test
	public void testGetUnderlyingSteps_noGroupBy_returnsSingleStep() {
		CubeQueryStep step = CubeQueryStep.builder().measure("m").filter(ISliceFilter.MATCH_ALL).build();

		List<? extends eu.solven.adhoc.engine.step.IWhereGroupByQuery> underlyingSteps =
				decomposition.getUnderlyingSteps(step);

		Assertions.assertThat(underlyingSteps).hasSize(1);
	}

	@Test
	public void testGetUnderlyingSteps_withGroupBy_addsElementColumn() {
		CubeQueryStep step = CubeQueryStep.builder()
				.measure("m")
				.filter(ISliceFilter.MATCH_ALL)
				.groupBy(GroupByColumns.named(GROUP_COL))
				.build();

		List<? extends eu.solven.adhoc.engine.step.IWhereGroupByQuery> underlyingSteps =
				decomposition.getUnderlyingSteps(step);

		Assertions.assertThat(underlyingSteps).hasSize(1);
		// The underlying step groups by the element column, not the group column
		Assertions.assertThat(underlyingSteps.get(0).getGroupBy().getSortedColumns()).contains(ELEMENT_COL);
		Assertions.assertThat(underlyingSteps.get(0).getGroupBy().getSortedColumns()).doesNotContain(GROUP_COL);
	}
}
