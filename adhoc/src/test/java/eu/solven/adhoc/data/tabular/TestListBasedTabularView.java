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
package eu.solven.adhoc.data.tabular;

import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import eu.solven.adhoc.data.row.slice.SliceAsMap;

public class TestListBasedTabularView {

	@Test
	public void testJackson() throws JsonProcessingException {
		ListBasedTabularView view = ListBasedTabularView.builder().build();

		view.appendSlice(SliceAsMap.fromMap(Map.of("c1", "v1")), Map.of("m", 123));

		String asString = TestMapBasedTabularView.verifyJackson(ListBasedTabularView.class, view);

		Assertions.assertThat(asString).isEqualTo("""
				{
				  "coordinates" : [ {
				    "c1" : "v1"
				  } ],
				  "values" : [ {
				    "m" : 123
				  } ]
				}""");

		Assertions.assertThat(view.toString()).isEqualTo("""
				ListBasedTabularView{size=1, #0=slice:{c1=v1}={m=123}}""");
	}

	@Test
	public void testEmpty() throws JsonProcessingException {
		Assertions.assertThat(ListBasedTabularView.empty().isEmpty()).isTrue();
	}

	@Test
	public void testLoad() {
		MapBasedTabularView mapBased =
				MapBasedTabularView.builder().coordinatesToValues(Map.of(Map.of("c", "c1"), Map.of("m", 123))).build();

		ListBasedTabularView loadedAsList = ListBasedTabularView.load(mapBased);
		ListBasedTabularView loadedAsList2 =
				ListBasedTabularView.load(mapBased, ListBasedTabularView.builder().build());

		Assertions.assertThat(loadedAsList).isEqualTo(loadedAsList2);

		Assertions.assertThat(loadedAsList.slices().toList()).hasSize(1).anySatisfy(slice -> {
			Assertions.assertThat(slice.getAdhocSliceAsMap().getCoordinates()).containsEntry("c", "c1").hasSize(1);
		});

		// Should not fail on a valid set of slices
		loadedAsList.checkIsDistinct();

	}

	@Test
	public void testDuplicateSlices() {
		ListBasedTabularView view =
				ListBasedTabularView.builder().coordinates(List.of(Map.of("c", "c1"), Map.of("c", "c1"))).build();

		Assertions.assertThatThrownBy(() -> view.checkIsDistinct())
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Multiple slices")
				.hasMessageContaining("c=c1");
	}

}
