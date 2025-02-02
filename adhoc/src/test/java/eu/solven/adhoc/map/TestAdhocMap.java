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
package eu.solven.adhoc.map;

import java.time.LocalDate;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAdhocMap {
	// Not a String to ensure we accept various types
	LocalDate now = LocalDate.now();

	@Test
	public void testGet() {
		IAdhocMap simpleMap = AdhocMap.builder(Set.of("a", "b")).append("a1").append(now).build();

		Assertions.assertThat(simpleMap).containsEntry("a", "a1").containsEntry("b", now).hasSize(2);

		Assertions.assertThat(simpleMap.containsKey("a")).isTrue();
		Assertions.assertThat(simpleMap.get("a")).isEqualTo("a1");

		Assertions.assertThat(simpleMap.containsKey("b")).isTrue();
		Assertions.assertThat(simpleMap.get("b")).isEqualTo(now);

		Assertions.assertThat(simpleMap.containsKey("c")).isFalse();
		Assertions.assertThat(simpleMap.get("c")).isEqualTo(null);
	}

	@Test
	public void testHashcode() {

		IAdhocMap simpleMap = AdhocMap.builder(Set.of("a", "b")).append("a1").append(now).build();
		IAdhocMap simpleMap2 = AdhocMap.builder(Set.of("a", "b")).append("a1").append(now).build();

		Assertions.assertThat(simpleMap).isEqualTo(simpleMap2);

		// Equals with standard map
		{
			Map<String, Object> asStandardMap = Map.of("a", "a1", "b", now);
			Assertions.assertThat(simpleMap).isEqualTo(asStandardMap);
			Assertions.assertThat(asStandardMap).isEqualTo(simpleMap);
			Assertions.assertThat(simpleMap.hashCode()).isEqualTo(asStandardMap.hashCode());

			Assertions.assertThat(simpleMap.entrySet()).isEqualTo(asStandardMap.entrySet());
		}
	}
}
