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
package eu.solven.adhoc.query.column_shift;

import java.util.Map;
import java.util.function.Function;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.MoreFilterHelpers;
import eu.solven.adhoc.query.filter.OrFilter;

public class TestSimpleFilterEditor {
	@Test
	public void testShiftAll() {
		ISliceFilter filter = ISliceFilter.MATCH_ALL;
		Assertions.assertThat(SimpleFilterEditor.shift(filter, "c", "v1")).isEqualTo(ColumnFilter.isEqualTo("c", "v1"));
	}

	@Test
	public void testShiftColumn() {
		ISliceFilter filter = ColumnFilter.isEqualTo("a", "a1");
		Assertions.assertThat(SimpleFilterEditor.shift(filter, "c", "v1"))
				.isEqualTo(AndFilter.and(Map.of("a", "a1", "c", "v1")));
	}

	@Test
	public void testShiftIfPresent() {
		ISliceFilter filter = ISliceFilter.MATCH_ALL;
		Assertions.assertThat(SimpleFilterEditor.shiftIfPresent(filter, "c", "v1")).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testMatchNone() {
		ISliceFilter filter = ISliceFilter.MATCH_NONE;
		Assertions.assertThat(SimpleFilterEditor.shift(filter, "c", "v1")).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testShift_And() {
		ISliceFilter filter = AndFilter.and(Map.of("a", "a1", "b", "b1", "c", "c1"));
		Assertions.assertThat(SimpleFilterEditor.shift(filter, "c", "c2"))
				.isEqualTo(AndFilter.and(Map.of("a", "a1", "b", "b1", "c", "c2")));
	}

	@Test
	public void testShift_Or() {
		ISliceFilter filter = OrFilter.or(Map.of("a", "a1", "b", "b1"));
		Assertions.assertThat(SimpleFilterEditor.shift(filter, "b", "b2"))
				.isEqualTo(
						FilterBuilder.or(AndFilter.and(Map.of("a", "a1", "b", "b2")), AndFilter.and(Map.of("b", "b2")))
								.optimize());
	}

	@Test
	public void testShiftIfPresent_Or() {
		ISliceFilter filter = OrFilter.or(Map.of("a", "a1", "b", "b1"));
		Assertions.assertThat(SimpleFilterEditor.shiftIfPresent(filter, "b", "b2"))
				.isEqualTo(FilterBuilder.or(AndFilter.and(Map.of("a", "a1")), AndFilter.and(Map.of("b", "b2")))
						.optimize());
	}

	@Test
	public void testShiftIfPresent_Or_function() {
		Function<Object, Object> shiftPreviousYear =
				rawYear -> rawYear instanceof Number year ? year.longValue() - 1 : rawYear;

		ISliceFilter filter = OrFilter.or(Map.of("a", "a1", "b", 123));
		ISliceFilter shiftedFilter = SimpleFilterEditor.shiftIfPresent(filter, "b", shiftPreviousYear);
		Assertions.assertThat(shiftedFilter).isInstanceOf(OrFilter.class);

		Assertions.assertThat(MoreFilterHelpers.match(shiftedFilter, Map.of())).isFalse();
		Assertions.assertThat(MoreFilterHelpers.match(shiftedFilter, Map.of("a", "a1"))).isTrue();
		Assertions.assertThat(MoreFilterHelpers.match(shiftedFilter, Map.of("b", 123))).isFalse();
		Assertions.assertThat(MoreFilterHelpers.match(shiftedFilter, Map.of("b", 122))).isTrue();
	}
}
