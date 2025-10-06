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
package eu.solven.adhoc.data.row;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.transformator.iterator.SlicedRecordFromArray;

public class TestISlicedRecord {
	ISlicedRecord slicedRecord = SlicedRecordFromArray.builder().measure("m1").measure("m2").build();

	@Test
	public void testAsList() {
		Assertions.assertThat((List) slicedRecord.asList()).containsExactly("m1", "m2");
	}

	@Test
	public void testIntoArray_shorter() {
		Object[] array = new Object[1];
		slicedRecord.intoArray(array);
		Assertions.assertThat(array).containsExactly("m1");
	}

	@Test
	public void testIntoArray_longer() {
		Object[] array = new Object[3];
		slicedRecord.intoArray(array);
		Assertions.assertThat(array).containsExactly("m1", "m2", null);
	}
}
