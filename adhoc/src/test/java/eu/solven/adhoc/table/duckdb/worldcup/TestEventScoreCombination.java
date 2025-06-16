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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.example.worldcup.EventsScoreCombination;

public class TestEventScoreCombination {
	EventsScoreCombination combination = new EventsScoreCombination();

	@Test
	public void testScore() {
		Assertions.assertThat(combination.combine(Mockito.mock(ISliceWithStep.class), Arrays.asList(5L, 1L)))
				.isEqualTo(4L);
		Assertions.assertThat(combination.combine(Mockito.mock(ISliceWithStep.class), Arrays.asList(5L, 2L)))
				.isEqualTo(1L);

	}

	@Test
	public void testScore_null() {
		Assertions.assertThat(combination.combine(Mockito.mock(ISliceWithStep.class), Arrays.asList(5L, null)))
				.isEqualTo(5L);
		Assertions.assertThat(combination.combine(Mockito.mock(ISliceWithStep.class), Arrays.asList(null, 2L)))
				.isEqualTo(-4L);

	}
}
