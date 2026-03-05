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
package eu.solven.adhoc.cube.training.a_basics;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.encoding.page.ColumnSliceFactory;
import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.map.factory.RowSliceFactory;

/**
 * Slices describes coordinates in a Cube. More specifically, they refer to a section of the cube.
 * 
 * Slices differs to filters, as filters describes a sub-cube given a WHERE clause.
 * 
 * @author Benoit Lacelle
 */
public class HelloSlices {
	@Test
	public void helloSlice() {
		// To build slice, you need a slice factory. This is due to the fact we generate a lot of slices with the same
		// keySet, so we can optimize their creation.
		ISliceFactory factory = RowSliceFactory.builder().build();

		// slices are generally created by first defining their keySet
		IAdhocMap slice = factory.newMapBuilder(List.of("color", "ccy")).append("blue", "EUR").build();

		// IAdhocMap is compatible with Map
		Map<String, String> rawMap = ImmutableMap.of("color", "blue", "ccy", "EUR");
		Assertions.assertThat((Map) slice).isEqualTo(rawMap).hasSameHashCodeAs(rawMap).hasToString(rawMap.toString());
	}

	@Test
	public void helloSliceWithNULL() {
		ISliceFactory factory = RowSliceFactory.builder().build();

		// one may encounter a NULL coordinate (e.g. on a failed JOIN)
		IAdhocMap slice = factory.newMapBuilder(List.of("color", "ccy")).append("blue", null).build();

		// IAdhocMap is compatible with Map
		Map<String, String> rawMap = new LinkedHashMap<>();
		rawMap.put("color", "blue");
		rawMap.put("ccy", null);

		// acknowledge the internal structure has a special NULL placeholder
		Assertions.assertThat((Map) slice).isEqualTo(rawMap).hasSameHashCodeAs(rawMap).hasToString(rawMap.toString());
	}

	@Test
	public void helloColumnar() {
		// A typical optimization is to rely on columnar storage, given we create many slices with the same keySet
		ISliceFactory factory = ColumnSliceFactory.builder().build();

		IAdhocMap slice = factory.newMapBuilder(List.of("color", "ccy")).append("blue", "EUR").build();

		Map<String, String> rawMap = ImmutableMap.of("color", "blue", "ccy", "EUR");
		Assertions.assertThat((Map) slice).isEqualTo(rawMap).hasSameHashCodeAs(rawMap).hasToString(rawMap.toString());
	}
}
