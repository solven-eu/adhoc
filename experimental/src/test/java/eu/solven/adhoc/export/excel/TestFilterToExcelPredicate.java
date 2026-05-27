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
package eu.solven.adhoc.export.excel;

import java.util.List;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.NotFilter;
import eu.solven.adhoc.filter.OrFilter;

public class TestFilterToExcelPredicate {

	/** Test {@link RowContext} that maps column → cell ref by lookup, with no underlying refs. */
	private static RowContext ctx(java.util.Map<String, String> columnToRef) {
		return new RowContext() {
			@Override
			public List<String> getUnderlyingCellRefs() {
				return List.of();
			}

			@Override
			public String getGroupByCellRef(String columnName) {
				String ref = columnToRef.get(columnName);
				if (ref == null) {
					throw new IllegalArgumentException("no ref for " + columnName);
				}
				return ref;
			}

			@Override
			public Set<String> getGroupByColumns() {
				return columnToRef.keySet();
			}
		};
	}

	@Test
	public void testEqualsString() {
		ISliceFilter f = ColumnFilter.matchEq("a", "a1");
		Assertions.assertThat(FilterToExcelPredicate.compile(f, ctx(java.util.Map.of("a", "A2"))))
				.isEqualTo("A2=\"a1\"");
	}

	@Test
	public void testEqualsNumber() {
		ISliceFilter f = ColumnFilter.matchEq("a", 42);
		Assertions.assertThat(FilterToExcelPredicate.compile(f, ctx(java.util.Map.of("a", "A2")))).isEqualTo("A2=42");
	}

	@Test
	public void testEqualsBoolean() {
		ISliceFilter f = ColumnFilter.matchEq("a", true);
		Assertions.assertThat(FilterToExcelPredicate.compile(f, ctx(java.util.Map.of("a", "A2")))).isEqualTo("A2=TRUE");
	}

	@Test
	public void testEqualsStringWithEmbeddedQuote() {
		ISliceFilter f = ColumnFilter.matchEq("a", "he said \"hi\"");
		Assertions.assertThat(FilterToExcelPredicate.compile(f, ctx(java.util.Map.of("a", "A2"))))
				.isEqualTo("A2=\"he said \"\"hi\"\"\"");
	}

	@Test
	public void testInMatcher_multiValue() {
		ISliceFilter f = ColumnFilter.matchIn("a", Set.of("a1", "a2"));
		String compiled = FilterToExcelPredicate.compile(f, ctx(java.util.Map.of("a", "A2")));
		Assertions.assertThat(compiled).startsWith("OR(").contains("A2=\"a1\"").contains("A2=\"a2\"").endsWith(")");
	}

	@Test
	public void testInMatcher_singleValueDegenerates() {
		ISliceFilter f = ColumnFilter.matchIn("a", Set.of("a1"));
		Assertions.assertThat(FilterToExcelPredicate.compile(f, ctx(java.util.Map.of("a", "A2"))))
				.isEqualTo("A2=\"a1\"");
	}

	@Test
	public void testAndFilter() {
		ISliceFilter f = AndFilter.and(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("b", "b1"));
		String compiled = FilterToExcelPredicate.compile(f, ctx(java.util.Map.of("a", "A2", "b", "B2")));
		Assertions.assertThat(compiled).isEqualTo("AND(A2=\"a1\",B2=\"b1\")");
	}

	@Test
	public void testOrFilter() {
		ISliceFilter f = OrFilter.or("a", "a1", "b", "b1");
		String compiled = FilterToExcelPredicate.compile(f, ctx(java.util.Map.of("a", "A2", "b", "B2")));
		Assertions.assertThat(compiled).startsWith("OR(").contains("A2=\"a1\"").contains("B2=\"b1\"").endsWith(")");
	}

	@Test
	public void testNotFilter() {
		ISliceFilter f = NotFilter.builder().negated(ColumnFilter.matchEq("a", "a1")).build();
		Assertions.assertThat(FilterToExcelPredicate.compile(f, ctx(java.util.Map.of("a", "A2"))))
				.isEqualTo("NOT(A2=\"a1\")");
	}

	@Test
	public void testColumnsReferenced_simple() {
		Assertions.assertThat(FilterToExcelPredicate.columnsReferenced(ColumnFilter.matchEq("a", "a1")))
				.containsExactly("a");
	}

	@Test
	public void testColumnsReferenced_andOrNot() {
		ISliceFilter f = AndFilter.and(ColumnFilter.matchEq("a", "a1"),
				NotFilter.builder().negated(ColumnFilter.matchEq("b", "b1")).build());
		Assertions.assertThat(FilterToExcelPredicate.columnsReferenced(f)).containsExactlyInAnyOrder("a", "b");
	}
}
