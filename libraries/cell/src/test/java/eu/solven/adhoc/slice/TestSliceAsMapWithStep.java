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
package eu.solven.adhoc.slice;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.cuboid.slice.SliceHelpers;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.SliceAsMapWithStep;
import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestSliceAsMapWithStep {
	@Test
	public void testAsFilter() {
		IMeasure k1Sum = Mockito.mock(IMeasure.class);

		ISliceFilter stepFilter = ColumnFilter.matchEq("c1", "v1");
		CubeQueryStep step =
				CubeQueryStep.builder().measure(k1Sum).filter(stepFilter).groupBy(GroupByColumns.named("c2")).build();
		ISlice parentSlice = SliceHelpers.asSlice(Map.of("c2", "v2"));

		SliceAsMapWithStep slice = SliceAsMapWithStep.builder().queryStep(step).slice(parentSlice).build();

		Assertions.assertThat(slice.asFilter()).isEqualTo(AndFilter.and("c1", "v1", "c2", "v2"));
	}

	@Test
	public void testToString() {
		IMeasure k1Sum = Mockito.mock(IMeasure.class);

		ISliceFilter stepFilter = ColumnFilter.matchEq("c1", "v1");
		CubeQueryStep step =
				CubeQueryStep.builder().measure(k1Sum).filter(stepFilter).groupBy(GroupByColumns.named("c2")).build();
		ISlice parentSlice = SliceHelpers.asSlice(Map.of("c2", "v2"));

		SliceAsMapWithStep slice = SliceAsMapWithStep.builder().queryStep(step).slice(parentSlice).build();

		String asString = slice.toString();
		log.debug("SliceAsMapWithStep.toString() = {}", asString);

		// The wrapper's toString must include the slice and query-step content that actually identifies it.
		Assertions.assertThat(asString)
				.startsWith("SliceAsMapWithStep(")
				.contains("slice=slice:{c2=v2}")
				.contains("filter=c1==v1")
				.contains("groupBy=(c2)");

		// The memoized `filterSupplier` field must NOT leak its Guava wrapper / lambda reference into toString:
		// those strings are meaningless noise for a human reader and the filter content is already derivable
		// from `slice` + `queryStep`.
		Assertions.assertThat(asString).doesNotContain("Suppliers.memoize").doesNotContain("$$Lambda");
	}
}
