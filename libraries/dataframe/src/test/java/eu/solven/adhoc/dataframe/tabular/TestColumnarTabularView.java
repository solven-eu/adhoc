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
package eu.solven.adhoc.dataframe.tabular;

import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestColumnarTabularView {

	@Test
	public void testEmpty() {
		ColumnarTabularView view = ColumnarTabularView.builder().build();

		Assertions.assertThat(view.isEmpty()).isTrue();
		Assertions.assertThat(view.size()).isEqualTo(0);
		Assertions.assertThat(view.slices().toList()).isEmpty();
	}

	@Test
	public void testAppendRow() {
		ColumnarTabularView view = ColumnarTabularView.builder().build();

		view.appendRow(Map.of("c1", "v1"), Map.of("m", 123));

		Assertions.assertThat(view.size()).isEqualTo(1);
		Assertions.assertThat(view.isEmpty()).isFalse();
		Assertions.assertThat(view.getCoordinateColumns()).containsKey("c1");
		Assertions.assertThat(view.getCoordinateColumns().get("c1")).isEqualTo(List.of("v1"));
		Assertions.assertThat(view.getAggregateColumns()).containsKey("m");
		Assertions.assertThat(view.getAggregateColumns().get("m")).isEqualTo(List.of(123));
	}

	@Test
	public void testAppendRow_multipleRows() {
		ColumnarTabularView view = ColumnarTabularView.builder().build();

		view.appendRow(Map.of("c1", "v1"), Map.of("m", 10));
		view.appendRow(Map.of("c1", "v2"), Map.of("m", 20));

		Assertions.assertThat(view.size()).isEqualTo(2);
		Assertions.assertThat(view.getCoordinateColumns().get("c1")).isEqualTo(List.of("v1", "v2"));
		Assertions.assertThat(view.getAggregateColumns().get("m")).isEqualTo(List.of(10, 20));
	}

	@Test
	public void testSlices() {
		ColumnarTabularView view = ColumnarTabularView.builder().build();

		view.appendRow(Map.of("c1", "v1"), Map.of("m", 123));

		Assertions.assertThat(view.slices().toList()).hasSize(1).anySatisfy(slice -> {
			Assertions.assertThat((Map) slice.asAdhocMap()).containsEntry("c1", "v1").hasSize(1);
		});
	}

	@Test
	public void testLoad_fromMapBased() {
		MapBasedTabularView mapBased =
				MapBasedTabularView.builder().coordinatesToValues(Map.of(Map.of("c", "c1"), Map.of("m", 123))).build();

		ColumnarTabularView loaded = ColumnarTabularView.load(mapBased);

		Assertions.assertThat(loaded.size()).isEqualTo(1);
		Assertions.assertThat(loaded.getCoordinateColumns().get("c")).isEqualTo(List.of("c1"));
		Assertions.assertThat(loaded.getAggregateColumns().get("m")).isEqualTo(List.of(123));
	}

	@Test
	public void testLoad_alreadyColumnar() {
		ColumnarTabularView original = ColumnarTabularView.builder().build();
		original.appendRow(Map.of("c1", "v1"), Map.of("m", 123));

		ColumnarTabularView loaded = ColumnarTabularView.load(original);

		Assertions.assertThat(loaded).isSameAs(original);
	}

	@Test
	public void testLoad_fromListBased() {
		ListBasedTabularView listBased = ListBasedTabularView.builder()
				.coordinates(List.of(Map.of("c", "c1")))
				.values(List.of(Map.of("m", 42)))
				.build();

		ColumnarTabularView loaded = ColumnarTabularView.load(listBased);

		Assertions.assertThat(loaded.size()).isEqualTo(1);
		Assertions.assertThat(loaded.getCoordinateColumns().get("c")).isEqualTo(List.of("c1"));
		Assertions.assertThat(loaded.getAggregateColumns().get("m")).isEqualTo(List.of(42));
	}

	@Test
	public void testStream() {
		ColumnarTabularView view = ColumnarTabularView.builder().build();
		view.appendRow(Map.of("c1", "v1"), Map.of("m", 123));

		List<Map<String, Object>> rows = view.<Map<String, Object>>stream(slice -> aggRow -> {
			Map<String, Object> merged = new java.util.LinkedHashMap<>(slice.asAdhocMap());
			merged.putAll(aggRow);
			return merged;
		}).toList();

		Assertions.assertThat(rows).hasSize(1).anySatisfy(row -> {
			Assertions.assertThat(row).containsEntry("c1", "v1").containsEntry("m", 123);
		});
	}

	@Test
	public void testCheckIsDistinct_valid() {
		ColumnarTabularView view = ColumnarTabularView.builder().build();
		view.appendRow(Map.of("c1", "v1"), Map.of("m", 1));
		view.appendRow(Map.of("c1", "v2"), Map.of("m", 2));

		// Should not throw
		view.checkIsDistinct();
	}

	@Test
	public void testCheckIsDistinct_duplicate() {
		ColumnarTabularView view = ColumnarTabularView.builder().build();
		view.appendRow(Map.of("c1", "v1"), Map.of("m", 1));
		view.appendRow(Map.of("c1", "v1"), Map.of("m", 2));

		Assertions.assertThatThrownBy(view::checkIsDistinct)
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Multiple slices")
				.hasMessageContaining("c1=v1");
	}

	@Test
	public void testAcceptScanner() {
		ColumnarTabularView view = ColumnarTabularView.builder().build();
		view.appendRow(Map.of("c1", "v1"), Map.of("m", 99));

		List<String> seen = new java.util.ArrayList<>();
		view.acceptScanner(slice -> aggRow -> seen.add(slice.asAdhocMap() + "=" + aggRow));

		Assertions.assertThat(seen).containsExactly("{c1=v1}={m=99}");
	}
}
