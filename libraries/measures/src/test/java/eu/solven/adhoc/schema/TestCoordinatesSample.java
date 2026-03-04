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
package eu.solven.adhoc.schema;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.beta.schema.CoordinatesSample;

public class TestCoordinatesSample {
	@Test
	public void testNoEstimation() {
		CoordinatesSample sample = CoordinatesSample.builder().coordinates(Set.of("a", "b")).build();

		Assertions.assertThat(sample.getCoordinates()).isEqualTo(Set.of("a", "b"));

		// BEWARE Should we correct the estimation given the sample?
		// No, as estimating with `2` while the size is 2 millions could lead to instabilities
		Assertions.assertThat(sample.getEstimatedCardinality()).isEqualTo(-1);
	}

	@Test
	public void testHigherEstimation() {
		CoordinatesSample sample =
				CoordinatesSample.builder().coordinates(Set.of("a", "b")).estimatedCardinality(123).build();

		Assertions.assertThat(sample.getCoordinates()).isEqualTo(Set.of("a", "b"));
		Assertions.assertThat(sample.getEstimatedCardinality()).isEqualTo(123);
	}

	@Test
	public void testUndersizedEstimation() {
		CoordinatesSample sample =
				CoordinatesSample.builder().coordinates(Set.of("a", "b", "c")).estimatedCardinality(1).build();

		Assertions.assertThat(sample.getCoordinates()).isEqualTo(Set.of("a", "b", "c"));
		// BEWARE Should we correct the estimation given the sample?
		Assertions.assertThat(sample.getEstimatedCardinality()).isEqualTo(1);
	}
}
