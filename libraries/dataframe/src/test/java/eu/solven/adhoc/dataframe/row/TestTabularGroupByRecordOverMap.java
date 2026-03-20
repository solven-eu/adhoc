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
package eu.solven.adhoc.dataframe.row;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.cuboid.slice.SliceHelpers;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestTabularGroupByRecordOverMap {
	@Test
	public void testToString() {
		String asString = TabularGroupByRecordOverMap.toString(TabularGroupByRecordOverMap.builder()
				.groupBy(GroupByColumns.named("k"))
				.slice(SliceHelpers.asSlice(Map.of("k", "v")))
				.build());

		Assertions.assertThat(asString).isEqualTo("slice:{k=v}");
	}

	@Test
	public void testRequireFilter_String() {
		ISlice slice = SliceHelpers.asSlice(Map.of("k", "v"));
		TabularGroupByRecordOverMap tabular =
				TabularGroupByRecordOverMap.builder().groupBy(GroupByColumns.named("k")).slice(slice).build();

		Assertions.assertThat(tabular.columnsKeySet()).containsExactly("k");

		{
			Assertions.assertThat(tabular.getGroupBy("k")).isEqualTo("v");
			Assertions.assertThat(tabular.getGroupBy("k", Object.class)).isEqualTo("v");
			Assertions.assertThat(tabular.getGroupBy("k", String.class)).isEqualTo("v");
			Assertions.assertThatThrownBy(() -> tabular.getGroupBy("k", Number.class))
					.isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThat(tabular.optGroupBy("k")).contains("v");
		}

		{
			Assertions.assertThatThrownBy(() -> tabular.getGroupBy("k2")).isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThatThrownBy(() -> tabular.getGroupBy("k2", Object.class))
					.isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThatThrownBy(() -> tabular.getGroupBy("k2", Number.class))
					.isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThat(tabular.optGroupBy("k2")).isEmpty();
		}
	}

	// @Disabled("AdhocSliceAsMap does not accept Collection sliced")
	@Test
	public void testRequireFilter_Collection() {
		ISlice slice = SliceHelpers.asSlice(Map.of("k", Arrays.asList("v1", "v2")));
		TabularGroupByRecordOverMap tabular =
				TabularGroupByRecordOverMap.builder().groupBy(GroupByColumns.named("k")).slice(slice).build();

		Assertions.assertThat(tabular.columnsKeySet()).containsExactly("k");

		{
			Assertions.assertThat(tabular.getGroupBy("k")).isEqualTo(Arrays.asList("v1", "v2"));
			Assertions.assertThat(tabular.getGroupBy("k", Object.class)).isEqualTo(Arrays.asList("v1", "v2"));
			Assertions.assertThat(tabular.getGroupBy("k", Collection.class)).isEqualTo(Arrays.asList("v1", "v2"));
			Assertions.assertThat(tabular.getGroupBy("k", List.class)).isEqualTo(Arrays.asList("v1", "v2"));
			Assertions.assertThatThrownBy(() -> tabular.getGroupBy("k", String.class))
					.isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThatThrownBy(() -> tabular.getGroupBy("k", Number.class))
					.isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThat(tabular.optGroupBy("k")).contains(Arrays.asList("v1", "v2"));
		}

		{
			Assertions.assertThatThrownBy(() -> tabular.getGroupBy("k2")).isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThatThrownBy(() -> tabular.getGroupBy("k2", Object.class))
					.isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThatThrownBy(() -> tabular.getGroupBy("k2", Number.class))
					.isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThat(tabular.optGroupBy("k2")).isEmpty();
		}
	}
}
