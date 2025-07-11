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
package eu.solven.adhoc.table.duckdb.worldcup;

import java.util.Arrays;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.SliceAsMapWithStep;
import eu.solven.adhoc.example.worldcup.EventsScoreCombination;

public class TestEventScoreCombination {
	EventsScoreCombination combination = new EventsScoreCombination();

	SliceAsMapWithStep slice = SliceAsMapWithStep.builder()
			.queryStep(Mockito.mock(CubeQueryStep.class))
			.slice(SliceAsMap.fromMap(Map.of()))
			.build();

	@Test
	public void testScore() {
		Assertions.assertThat(combination.combine(slice, Arrays.asList(5L, 1L, 1L))).isEqualTo(4.0D);
		Assertions.assertThat(combination.combine(slice, Arrays.asList(5L, 2L, 1L))).isEqualTo(1.0D);

	}

	@Test
	public void testScore_null() {
		Assertions.assertThat(combination.combine(slice, Arrays.asList(5L, null, 1L))).isEqualTo(5.0D);
		Assertions.assertThat(combination.combine(slice, Arrays.asList(null, 2L, 1L))).isEqualTo(-4.0D);

	}
}
