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
package eu.solven.adhoc.storage;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;

import eu.solven.adhoc.slice.SliceAsMap;

public class TestMapBasedTabularView {

	@Test
	public void testJackson_KO() throws JsonProcessingException {
		MapBasedTabularView view = MapBasedTabularView.builder().build();

		view.appendSlice(SliceAsMap.fromMap(Map.of("c1", "v1")), "m", 123);

		Assertions.assertThatThrownBy(() -> verifyJackson(MapBasedTabularView.class, view))
				.isInstanceOf(InvalidDefinitionException.class);
	}

	public static <T> String verifyJackson(Class<T> clazz, T object) throws JsonProcessingException {
		ObjectMapper om = new ObjectMapper();

		String asString = om.writeValueAsString(object);

		Object fromString = om.readValue(asString, clazz);

		Assertions.assertThat(fromString).isEqualTo(object);

		return asString;
	}
}
