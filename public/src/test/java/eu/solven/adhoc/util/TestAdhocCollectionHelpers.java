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
package eu.solven.adhoc.util;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAdhocCollectionHelpers {
	LocalDate now = LocalDate.now();

	@Test
	public void testUnnest_noNested() {
		List<Object> noNested = Arrays.asList(123, 12.34, "foo", now);
		Assertions.assertThat(AdhocCollectionHelpers.unnestAsList(noNested)).isEqualTo(noNested);
	}

	@Test
	public void testUnnest_variousDepth() {
		List<Object> noNested = Arrays.asList(123, Arrays.asList(12.34, Arrays.asList(Arrays.asList("foo"), now)));
		Assertions.assertThat(AdhocCollectionHelpers.unnestAsList(noNested))
				.isEqualTo(Arrays.asList(123, 12.34D, "foo", now));
	}

	@Test
	public void testUnnest_null() {
		List<Object> noNested = Arrays.asList(123, null, Arrays.asList(null, "foo"));
		Assertions.assertThat(AdhocCollectionHelpers.unnestAsList(noNested))
				.isEqualTo(Arrays.asList(123, null, null, "foo"));
	}
}
