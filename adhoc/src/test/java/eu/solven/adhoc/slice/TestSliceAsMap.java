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
package eu.solven.adhoc.slice;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.map.AdhocMapHelpers;

public class TestSliceAsMap {
	@Test
	public void testRequireFilter_String() {
		IAdhocSlice slice = SliceAsMap.fromMap(Map.of("k", "v"));

		Assertions.assertThat(slice.getColumns()).containsExactly("k");

		{
			Assertions.assertThat(slice.getRawSliced("k")).isEqualTo("v");
			Assertions.assertThat(slice.getSliced("k", Object.class)).isEqualTo("v");
			Assertions.assertThat(slice.getSliced("k", String.class)).isEqualTo("v");
			Assertions.assertThatThrownBy(() -> slice.getSliced("k", Number.class))
					.isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThat(slice.optSliced("k")).contains("v");
		}

		{
			Assertions.assertThatThrownBy(() -> slice.getRawSliced("k2")).isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThatThrownBy(() -> slice.getSliced("k2", Object.class))
					.isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThatThrownBy(() -> slice.getSliced("k2", Number.class))
					.isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThat(slice.optSliced("k2")).isEmpty();
		}
	}

	@Disabled("AdhocSliceAsMap does not accept Collection sliced")
	@Test
	public void testRequireFilter_Collection() {
		IAdhocSlice slice = SliceAsMap.fromMap(Map.of("k", Arrays.asList("v1", "v2")));

		Assertions.assertThat(slice.getColumns()).containsExactly("k");

		{
			Assertions.assertThat(slice.getRawSliced("k")).isEqualTo(Arrays.asList("v1", "v2"));
			Assertions.assertThat(slice.getSliced("k", Object.class)).isEqualTo(Arrays.asList("v1", "v2"));
			Assertions.assertThat(slice.getSliced("k", Collection.class)).isEqualTo(Arrays.asList("v1", "v2"));
			Assertions.assertThat(slice.getSliced("k", List.class)).isEqualTo(Arrays.asList("v1", "v2"));
			Assertions.assertThatThrownBy(() -> slice.getSliced("k", String.class))
					.isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThatThrownBy(() -> slice.getSliced("k", Number.class))
					.isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThat(slice.optSliced("k")).contains(Arrays.asList("v1", "v2"));
		}

		{
			Assertions.assertThatThrownBy(() -> slice.getRawSliced("k2")).isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThatThrownBy(() -> slice.getSliced("k2", Object.class))
					.isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThatThrownBy(() -> slice.getSliced("k2", Number.class))
					.isInstanceOf(IllegalArgumentException.class);
			Assertions.assertThat(slice.optSliced("k2")).isEmpty();
		}
	}

	@Test
	public void testAddColumn() {
		IAdhocSlice slice = SliceAsMap.fromMap(Map.of("k", "v"));

		IAdhocSlice extended = slice.addColumns(Map.of("k2", "v2"));
		Assertions.assertThat((Map) extended.getCoordinates())
				.hasSize(2)
				.containsEntry("k", "v")
				.containsEntry("k2", "v2");
	}

	@Test
	public void testAddColumn_overlap() {
		IAdhocSlice slice = SliceAsMap.fromMap(Map.of("k", "v"));

		Assertions.assertThatThrownBy(() -> slice.addColumns(Map.of("k", "v2")))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("k", "v2");
	}

	// Typically happens with nullable columns
	@Test
	public void testCompare_differentType() {
		IAdhocSlice sliceDate = SliceAsMap.fromMap(Map.of("d", LocalDate.now()));
		IAdhocSlice sliceString = SliceAsMap.fromMap(Map.of("d", "NULL"));

		Assertions.assertThat(sliceDate).isGreaterThan(sliceString);
	}

	@Test
	public void testIntAndLong_notAdhocMap() {
		IAdhocSlice sliceInt = SliceAsMap.fromMap(Map.of("k", 123));
		IAdhocSlice sliceLong = SliceAsMap.fromMap(Map.of("k", 123L));

		Assertions.assertThat(sliceInt).isEqualTo(sliceLong);
	}

	@Test
	public void testIntAndLong_adhocMap() {
		IAdhocSlice sliceInt = SliceAsMap.fromMap(AdhocMapHelpers.wrap(Map.of("k", 123)));
		IAdhocSlice sliceLong = SliceAsMap.fromMap(AdhocMapHelpers.wrap(Map.of("k", 123L)));

		Assertions.assertThat(sliceInt).isEqualTo(sliceLong);
	}

	@Test
	public void testKeepAdhocMap() {
		IAdhocSlice sliceOverAdhocMap = SliceAsMap.fromMap(AdhocMapHelpers.wrap(Map.of("k", 123)));

		Assertions.assertThat(sliceOverAdhocMap.getCoordinates().getClass().getName())
				.isEqualTo("com.google.common.collect.Maps$TransformedEntriesMap");
	}
}
